"""Recording topic identities for an existing cutover only while broker history matches."""

from concurrent.futures import Future
from contextlib import contextmanager
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from keep import kafka_cutover
from keep.shared.errors import AppError
from test_source_topic_incarnation import CHANGED, RECEIVED, halves, signed

ENV = "development"
TOPICS = {stream: f"keep.{ENV}.alerts.{stream}" for stream in ("received", "changed")}
RETAINED = b"retained-input-11"


class Database:
    """Scripted control row and recorded history; captures the identity update."""

    def __init__(self, state, positions, results):
        self.state, self.positions, self.results = state, positions, results
        self.updates = []

    def execute(self, sql, params=()):
        if "SELECT * FROM kafka_pipeline_control" in sql:
            return Mock(fetchone=lambda: dict(self.state))
        if "FROM kafka_consumer_positions" in sql:
            return iter(self.positions)
        if "FROM kafka_source_results" in sql:
            return iter(self.results)
        assert sql.startswith("UPDATE kafka_pipeline_control"), sql
        self.updates.append(params)
        return Mock(fetchone=lambda: {"singleton": True})

    @contextmanager
    def transaction(self, *args, **kwargs):
        yield self


def identity(text):
    msb, lsb = halves(text)
    future = Future()
    future.set_result(
        SimpleNamespace(
            topic_id=SimpleNamespace(
                get_most_significant_bits=lambda: signed(msb),
                get_least_significant_bits=lambda: signed(lsb),
            )
        )
    )
    return future


def broker(monkeypatch, *, high=None, counts=None, record=RETAINED, members=()):
    """Stand-in admin and probe consumer for the local single-partition topics."""
    high = {"received": 12, "changed": 40, **(high or {})}
    counts = {"received": 1, "changed": 1, **(counts or {})}
    admin = Mock()
    group = Future()
    group.set_result(SimpleNamespace(members=list(members), state=None))
    admin.describe_consumer_groups.return_value = {"source": group} if members else {}
    ids = {TOPICS["received"]: RECEIVED, TOPICS["changed"]: CHANGED}
    admin.describe_topics.side_effect = lambda topics, **_: {
        name: identity(ids[name]) for name in topics.topic_names
    }
    consumer = Mock()
    consumer.list_topics.return_value = SimpleNamespace(
        topics={
            TOPICS[stream]: SimpleNamespace(error=None, partitions=dict.fromkeys(range(count)))
            for stream, count in counts.items()
        }
    )
    by_topic = {TOPICS[stream]: (0, end) for stream, end in high.items()}
    consumer.get_watermark_offsets.side_effect = lambda point, timeout: by_topic[point.topic]
    message = Mock(error=lambda: None, offset=lambda: 11, value=lambda: record)
    consumer.poll.return_value = message
    make_consumer = Mock(return_value=consumer)
    monkeypatch.setattr("confluent_kafka.admin.AdminClient", Mock(return_value=admin))
    monkeypatch.setattr("confluent_kafka.Consumer", make_consumer)
    monkeypatch.setattr("keep.service_clients.kafka_options", lambda settings: {})
    return admin, consumer, make_consumer


def database(recorded=(None, None)):
    """The local history after the 2026-10-25 reviewed reset (REV-40)."""
    return Database(
        {
            "enabled": True,
            "received_partitions": 1,
            "changed_partitions": 1,
            "received_topic_id": recorded[0],
            "changed_topic_id": recorded[1],
        },
        [
            {"topic": TOPICS["received"], "partition": 0, "next_offset": 12},
            {"topic": TOPICS["changed"], "partition": 0, "next_offset": 34},
        ],
        [
            {
                "topic": TOPICS["received"],
                "partition": 0,
                "input_offset": 11,
                "input_digest": sha256(RETAINED).digest(),
            }
        ],
    )


def record(db):
    return kafka_cutover.record_topic_identity(db, SimpleNamespace(environment=ENV))


def test_existing_cutover_records_topic_ids_once_history_matches(monkeypatch):
    """Requirements: REV-41, REV-40; Kafka alert pipeline 8."""
    admin, consumer, _ = broker(monkeypatch)
    db = database()
    assert record(db) == {
        "previous": {"received": None, "changed": None},
        "recorded": {"received": RECEIVED, "changed": CHANGED},
    }
    assert db.updates == [(RECEIVED, CHANGED, None, None)]
    # The retained record at the last source result was read, never committed.
    assigned = consumer.assign.call_args.args[0][0]
    assert (assigned.topic, assigned.partition, assigned.offset) == (TOPICS["received"], 0, 11)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


@pytest.mark.parametrize(
    ("broker_state", "code"),
    [
        # 2026-10-25: changed restarted and ended at 12, below indexer position 34.
        ({"high": {"changed": 12}}, "kafka_topic_history_mismatch"),
        # Received ends below the last source result PostgreSQL recorded.
        ({"high": {"received": 7}}, "kafka_topic_history_mismatch"),
        # Received holds another record at the last recorded coordinate.
        ({"record": b"another-input-at-11"}, "kafka_topic_history_mismatch"),
        ({"counts": {"received": 2}}, "kafka_partition_topology_changed"),
    ],
)
def test_lost_broker_history_is_never_recorded(monkeypatch, broker_state, code):
    """Requirements: REV-41, REV-40; Kafka alert pipeline 8."""
    broker(monkeypatch, **broker_state)
    db = database()
    with pytest.raises(AppError) as raised:
        record(db)
    assert raised.value.code == code
    assert db.updates == []


def test_active_pipeline_groups_block_recording(monkeypatch):
    """Requirements: REV-41; Kafka alert pipeline 8."""
    admin, _, make_consumer = broker(monkeypatch, members=["source-1"])
    db = database()
    with pytest.raises(AppError) as raised:
        record(db)
    assert raised.value.code == "cutover_consumers_active"
    admin.describe_topics.assert_not_called()
    make_consumer.assert_not_called()
    assert db.updates == []


def test_matching_recorded_identity_reads_no_history(monkeypatch):
    """Requirements: REV-41; Kafka alert pipeline 8."""
    _, _, make_consumer = broker(monkeypatch)
    # Recorded by a client that reports 32 bits of each half (Windows).
    db = database(("_____-uHT_j_____jd3lUQ", CHANGED))
    result = record(db)
    assert result["recorded"] == result["previous"]
    make_consumer.assert_not_called()
    assert db.updates == []


def test_recording_requires_a_cutover(monkeypatch):
    """Requirements: REV-41; Kafka alert pipeline 8."""
    admin, _, _ = broker(monkeypatch)
    db = database()
    db.state["enabled"] = False
    with pytest.raises(AppError) as raised:
        record(db)
    assert raised.value.code == "kafka_cutover_required"
    admin.describe_consumer_groups.assert_not_called()
