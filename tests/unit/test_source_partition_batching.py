"""Source partition batches: per-input PostgreSQL proof, one Kafka transaction per batch."""

from contextlib import contextmanager
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4
import json

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from appchi import clock_health, kafka_source
from appchi.kafka_changed import PartitionRetries
from appchi.kafka_source import SourceConsumer
from appchi.shared.errors import AppError, unavailable

TOPIC = "appchi.test.alerts.received"
CHANGED = "appchi.test.alerts.changed"
TENANT = uuid4()


def record(partition, offset, value=None):
    value = value or f"input-{partition}-{offset}".encode()
    return Mock(
        topic=lambda: TOPIC,
        partition=lambda: partition,
        offset=lambda: offset,
        error=lambda: None,
        key=lambda: b"key",
        value=lambda: value,
    )


def changed(partition, offset):
    return json.dumps(
        {"tenant_id": str(TENANT), "event_id": str(uuid4()), "input": [partition, offset]}
    ).encode()


class Database:
    """Each transaction gets its own connection so commit boundaries are visible."""

    def __init__(self):
        self.transactions = []

    @contextmanager
    def transaction(self, *args, **kwargs):
        conn = Mock()
        self.transactions.append(conn)
        yield conn

    def statements(self):
        return [[call.args for call in conn.execute.call_args_list] for conn in self.transactions]


def source(monkeypatch, fetches, apply, partitions=(0, 1)):
    monkeypatch.setattr(clock_health, "require_clock_health", lambda *args: None)
    monkeypatch.setattr(kafka_source, "apply_record", Mock(side_effect=apply))
    monkeypatch.setattr(kafka_source, "monotonic", lambda: 0.0)
    worker = SourceConsumer.__new__(SourceConsumer)
    worker.db = Database()
    worker.settings, worker.profile = SimpleNamespace(environment="test"), None
    worker.closing = False
    worker.client = Mock()
    worker.client.consume.side_effect = fetches
    worker.client.poll.return_value = None
    worker.retries = PartitionRetries(worker.client)
    worker.producers = {partition: Mock() for partition in partitions}
    return worker


def applied_offsets():
    return [
        (call.kwargs["partition"], call.kwargs["offset"])
        for call in kafka_source.apply_record.call_args_list
    ]


def transaction(producer):
    """Return (Kafka call names, produced values, committed next offset) for one producer."""
    names = [call[0] for call in producer.mock_calls]
    values = [call.kwargs["value"] for call in producer.produce.call_args_list]
    for call in producer.produce.call_args_list:
        assert call.args == (CHANGED,)
        assert call.kwargs["key"] == TENANT.bytes
    offsets = [call.args[0] for call in producer.send_offsets_to_transaction.call_args_list]
    return names, values, [(point.partition, point.offset) for batch in offsets for point in batch]


def test_partition_batch_publishes_events_in_order_with_next_offset_in_one_transaction(
    monkeypatch,
):
    events = {10: changed(0, 10), 12: changed(0, 12)}
    worker = source(
        monkeypatch,
        [[record(0, 10), record(0, 11), record(0, 12)]],
        lambda db, *, offset, **_: events.get(offset),
    )
    assert worker.turn() == 3
    worker.client.consume.assert_called_once_with(100, timeout=0)
    worker.client.poll.assert_not_called()
    assert applied_offsets() == [(0, 10), (0, 11), (0, 12)]
    names, values, offsets = transaction(worker.producers[0])
    # Offset 11 was rejected or replayed elsewhere: it emits nothing but advances.
    assert names == [
        "begin_transaction",
        "produce",
        "produce",
        "send_offsets_to_transaction",
        "commit_transaction",
    ]
    assert values == [events[10], events[12]]
    assert offsets == [(0, 13)]
    # Confirmation and position bookkeeping is one PostgreSQL transaction.
    [bookkeeping] = worker.db.statements()
    assert "kafka_source_results SET kafka_confirmed" in bookkeeping[0][0]
    assert bookkeeping[0][1] == (TOPIC, 0, 10, 13)
    assert "kafka_consumer_positions" in bookkeeping[1][0]
    assert bookkeeping[1][1] == (TOPIC, 0, 13)


def test_each_partition_gets_its_own_transaction_and_keeps_input_order(monkeypatch):
    worker = source(
        monkeypatch,
        [[record(0, 5), record(1, 7), record(0, 6), record(1, 8)]],
        lambda db, *, partition, offset, **_: changed(partition, offset),
    )
    assert worker.turn() == 4
    assert applied_offsets() == [(0, 5), (0, 6), (1, 7), (1, 8)]
    for partition, expected in ((0, [(0, 5), (0, 6)]), (1, [(1, 7), (1, 8)])):
        names, values, offsets = transaction(worker.producers[partition])
        assert names.count("begin_transaction") == names.count("commit_transaction") == 1
        assert [json.loads(value)["input"] for value in values] == [list(e) for e in expected]
        assert offsets == [(partition, expected[-1][1] + 1)]
    assert [statements[1][1] for statements in worker.db.statements()] == [
        (TOPIC, 0, 7),
        (TOPIC, 1, 9),
    ]


def test_idle_consumer_waits_for_a_single_record(monkeypatch):
    worker = source(monkeypatch, [[], []], lambda db, **_: None)
    assert worker.turn() == 0
    worker.client.poll.assert_called_once_with(0.1)
    worker.producers[0].begin_transaction.assert_not_called()
    worker.client.poll.return_value = record(0, 3)
    assert worker.turn() == 1
    assert transaction(worker.producers[0])[2] == [(0, 4)]


def test_batch_closes_after_the_application_time_bound(monkeypatch):
    events = []
    clock = [0.0]

    def apply(db, *, offset, **_):
        clock[0] += 0.04
        events.append(("apply", offset))
        return changed(0, offset)

    worker = source(monkeypatch, [[record(0, offset) for offset in range(5)]], apply)
    monkeypatch.setattr(kafka_source, "monotonic", lambda: clock[0])
    producer = worker.producers[0]
    producer.commit_transaction.side_effect = lambda *args: events.append(("commit",))
    assert worker.turn() == 5
    # 0.12 s of application closes the first batch before offset 3 is applied.
    assert events == [
        ("apply", 0),
        ("apply", 1),
        ("apply", 2),
        ("commit",),
        ("apply", 3),
        ("apply", 4),
        ("commit",),
    ]
    assert transaction(producer)[2] == [(0, 3), (0, 5)]
    assert [statements[0][1] for statements in worker.db.statements()] == [
        (TOPIC, 0, 0, 3),
        (TOPIC, 0, 3, 5),
    ]


def test_postgres_failure_mid_batch_commits_prefix_then_retains_failed_offset(monkeypatch):
    poison = record(0, 22)

    def apply(db, *, partition, offset, **_):
        if (partition, offset) == (0, 22):
            raise unavailable("source_identity_proof_invalid")
        return changed(partition, offset)

    worker = source(
        monkeypatch,
        [[record(0, 20), record(0, 21), poison, record(0, 23), record(1, 4)], [record(0, 23)]],
        apply,
    )
    order = []
    worker.producers[0].commit_transaction.side_effect = lambda *args: order.append("commit")
    worker.client.seek.side_effect = lambda point: order.append(("seek", point.offset))
    assert worker.turn() == 3
    # Offset 23 is never applied ahead of the retained failure.
    assert applied_offsets() == [(0, 20), (0, 21), (0, 22), (1, 4)]
    names, values, offsets = transaction(worker.producers[0])
    assert [json.loads(value)["input"] for value in values] == [[0, 20], [0, 21]]
    assert offsets == [(0, 22)]
    assert order == ["commit", ("seek", 22)]
    assert worker.client.pause.call_args.args[0][0].offset == 22
    assert (TOPIC, 0) in worker.retries.failed
    prefix, fault, healthy = worker.db.statements()
    assert prefix[1][1] == (TOPIC, 0, 22)
    assert "kafka_pipeline_faults" in fault[0][0]
    assert fault[0][1] == (
        TOPIC,
        0,
        22,
        "source_identity_proof_invalid",
        sha256(poison.value()).digest(),
    )
    assert healthy[1][1] == (TOPIC, 1, 5)
    assert transaction(worker.producers[1])[2] == [(1, 5)]
    # A stale fetched record of the retained partition is not applied.
    assert worker.turn() == 0
    assert applied_offsets()[-1] == (1, 4)


def test_failure_of_first_record_opens_no_kafka_transaction(monkeypatch):
    def apply(db, **_):
        raise ValueError("arbitrary exception text is not persisted")

    worker = source(monkeypatch, [[record(0, 8)]], apply)
    assert worker.turn() == 0
    assert worker.producers[0].mock_calls == []
    assert worker.client.seek.call_args.args[0].offset == 8
    [fault] = worker.db.statements()
    assert fault[0][1][3] == "source_processing_failed"


@pytest.mark.parametrize(
    "step", ["begin_transaction", "produce", "send_offsets_to_transaction", "commit_transaction"]
)
def test_uncertain_kafka_outcome_rejoins_without_confirming_the_batch(monkeypatch, step):
    worker = source(
        monkeypatch,
        [[record(0, 30), record(1, 40), record(1, 41)]],
        lambda db, *, partition, offset, **_: changed(partition, offset),
    )
    error = KafkaException(KafkaError(KafkaError._TIMED_OUT, "transaction outcome unknown"))
    getattr(worker.producers[1], step).side_effect = error
    with pytest.raises(KafkaException):
        worker.turn()
    # The earlier partition's committed batch keeps its bookkeeping; the uncertain
    # batch stays unconfirmed for replay and its offset is not retried locally.
    assert applied_offsets() == [(0, 30), (1, 40), (1, 41)]
    assert [statements[1][1] for statements in worker.db.statements()] == [(TOPIC, 0, 31)]
    worker.client.seek.assert_not_called()
    assert not worker.retries.failed


def test_rebalance_during_fetch_skips_revoked_partition(monkeypatch):
    def fetch(*args, **kwargs):
        worker._revoked(worker.client, [TopicPartition(TOPIC, 1)])
        return [record(0, 1), record(1, 9)]

    worker = source(monkeypatch, [], lambda db, *, partition, offset, **_: changed(0, offset))
    revoked = worker.producers[1]
    worker.client.consume.side_effect = fetch
    assert worker.turn() == 1
    assert applied_offsets() == [(0, 1)]
    assert revoked.mock_calls == []
    assert 1 not in worker.producers
    assert transaction(worker.producers[0])[2] == [(0, 2)]


def test_consume_error_rejoins_before_applying_any_record(monkeypatch):
    broken = record(1, 2)
    broken.error = lambda: KafkaError(KafkaError._TRANSPORT)
    worker = source(monkeypatch, [[record(0, 1), broken]], lambda db, **_: None)
    with pytest.raises(AppError) as raised:
        worker.turn()
    assert raised.value.code == "kafka_source_consume_failed"
    kafka_source.apply_record.assert_not_called()


def test_replay_at_same_coordinate_reemits_stored_bytes_once_unconfirmed(monkeypatch):
    """A stand-in source-result store driven by the real replay_result decision."""
    rows = {}

    def apply(db, *, topic, partition, offset, value, **_):
        digest = sha256(value).digest()
        if value in rows:
            return kafka_source.replay_result(rows[value], TENANT, digest, topic, partition, offset)
        rows[value] = {
            "input_digest": digest,
            "tenant_id": TENANT,
            "topic": topic,
            "partition": partition,
            "input_offset": offset,
            "changed_envelope": changed(partition, offset),
            "kafka_confirmed": False,
        }
        return rows[value]["changed_envelope"]

    def execute(sql, params):
        if "SET kafka_confirmed" in sql:
            topic, partition, first, end = params
            for row in rows.values():
                if (row["topic"], row["partition"]) == (topic, partition) and (
                    first <= row["input_offset"] < end
                ):
                    row["kafka_confirmed"] = True

    batch = [record(0, 50), record(0, 51)]
    first = source(monkeypatch, [batch], apply, partitions=(0,))
    first.producers[0].commit_transaction.side_effect = KafkaException(
        KafkaError(KafkaError._TIMED_OUT)
    )
    with pytest.raises(KafkaException):
        first.turn()
    stored = [rows[message.value()]["changed_envelope"] for message in batch]

    # The next owner resumes at the committed offset with a new fenced producer.
    duplicate = record(0, 60, value=batch[0].value())
    second = source(monkeypatch, [batch, [duplicate]], apply, partitions=(0,))
    second.db.transaction = contextmanager(
        lambda *args, **kwargs: (yield Mock(execute=Mock(side_effect=execute)))
    )
    assert second.turn() == 2
    names, values, offsets = transaction(second.producers[0])
    assert values == stored
    assert offsets == [(0, 52)]
    assert all(row["kafka_confirmed"] for row in rows.values())
    # The same delivery at another coordinate advances without another event.
    assert second.turn() == 1
    names, values, offsets = transaction(second.producers[0])
    assert values == stored
    assert offsets == [(0, 52), (0, 61)]


def test_replay_result_returns_bytes_only_at_unconfirmed_original_coordinate():
    digest = sha256(b"input").digest()
    row = {
        "input_digest": digest,
        "tenant_id": TENANT,
        "topic": TOPIC,
        "partition": 0,
        "input_offset": 7,
        "changed_envelope": b"changed",
        "kafka_confirmed": False,
    }
    replay = kafka_source.replay_result
    assert replay(row, TENANT, digest, TOPIC, 0, 7) == b"changed"
    assert replay(row, TENANT, digest, TOPIC, 0, 8) is None
    assert replay(dict(row, kafka_confirmed=True), TENANT, digest, TOPIC, 0, 7) is None
    for args in ((TENANT, sha256(b"other").digest()), (uuid4(), digest)):
        with pytest.raises(AppError) as raised:
            replay(row, *args, TOPIC, 0, 7)
        assert raised.value.code == "source_delivery_identity_conflict"
