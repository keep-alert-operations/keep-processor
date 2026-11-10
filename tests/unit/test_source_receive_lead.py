"""A receipt stamped ahead of the tenant clock waits; it is never applied early."""

from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4
import json
import logging

import pytest

from keep import clock_health, kafka_changed, kafka_source
from keep.kafka_changed import PartitionRetries
from keep.kafka_source import ReceiveTimeAhead, SourceConsumer

TOPIC = "keep.test.alerts.received"
TENANT = uuid4()
NOW = datetime(2026, 10, 19, 21, 26, 29, 792700, tzinfo=UTC)


class Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class LockedConn:
    """The coordinator lock, clock statement and the writes that must not happen early."""

    def __init__(self, clock):
        self.clock = clock
        self.statements = []

    def execute(self, query, params=()):
        text = " ".join(str(query).split())
        self.statements.append(text)
        if text.startswith("SELECT * FROM tenant_work_state") and "FOR UPDATE" in text:
            return Result([{"tenant_id": TENANT, "admission_order": 7, "last_clock": None}])
        if "UPDATE tenant_work_state t SET last_clock=c.now" in text:
            return Result([{"now": self.clock, "admission_order": 8}])
        if text.startswith("SELECT * FROM kafka_source_results"):
            return Result([])
        if text.startswith("SELECT retired_at FROM alert_retired_keys"):
            return Result([])
        if text.startswith("INSERT INTO kafka_source_results"):
            return Result([])
        raise AssertionError(f"unexpected statement: {text}")


def application(at):
    envelope = SimpleNamespace(
        event_id=str(uuid4()),
        operator_id=None,
        native_version="v1",
        raw_json="{}",
        time_defaulted=False,
        adapter_version=None,
    )
    return dict(
        tenant=TENANT,
        envelope=envelope,
        payload={"application": "a"},
        key_digest=b"k",
        material=b"m",
        logical_key="key",
        at=at,
        digest=sha256(b"value").digest(),
        topic=TOPIC,
        partition=0,
        offset=3,
        catalog=None,
    )


@pytest.fixture
def applied(monkeypatch):
    calls = []

    def fake_apply_one(conn, tenant, attempt, catalog, *, supplied_item=None, coordinated=None):
        calls.append(supplied_item)
        return {"alert_id": uuid4(), "error_code": None, "envelope": b"{}", "event_id": "evt"}

    monkeypatch.setattr("keep.alerts.processor._apply_one", fake_apply_one)
    return calls


def test_receipt_ahead_of_tenant_clock_raises_before_any_write(applied):
    # The integration run that found this: ingress led the database by ~10 ms.
    at = datetime(2026, 10, 19, 21, 26, 29, 802937, tzinfo=UTC)
    conn = LockedConn(NOW)
    with pytest.raises(ReceiveTimeAhead) as raised:
        kafka_source._apply_record_locked(conn, **application(at))
    assert raised.value.code == "source_receive_time_ahead"
    assert raised.value.status == 503
    assert raised.value.lead == pytest.approx(0.010237)
    # The clock/admission statement is the last one; the transaction rolls it
    # back. No source mutation, maintenance input or result row exists.
    assert "SET last_clock=c.now" in conn.statements[-1]
    assert not [text for text in conn.statements if text.startswith("INSERT")]
    assert applied == []


def test_receipt_equal_to_tenant_clock_applies(applied):
    conn = LockedConn(NOW)
    assert kafka_source._apply_record_locked(conn, **application(NOW)) == b"{}"
    (item,) = applied
    assert item["received_at"] == NOW
    assert conn.statements[-1].startswith("INSERT INTO kafka_source_results")


class Database:
    def __init__(self):
        self.transactions = []

    @contextmanager
    def transaction(self, *args, **kwargs):
        conn = Mock()
        self.transactions.append(conn)
        yield conn

    def statements(self):
        return [[call.args for call in conn.execute.call_args_list] for conn in self.transactions]


def record(partition, offset):
    value = f"input-{partition}-{offset}".encode()
    return Mock(
        topic=lambda: TOPIC,
        partition=lambda: partition,
        offset=lambda: offset,
        error=lambda: None,
        key=lambda: b"key",
        value=lambda: value,
    )


def changed(partition, offset):
    return kafka_source.ChangedEvent(
        json.dumps({"tenant_id": str(TENANT), "input": [partition, offset]}).encode(), TENANT
    )


@pytest.fixture
def clock(monkeypatch):
    now = SimpleNamespace(value=100.0)
    monkeypatch.setattr(kafka_changed, "monotonic", lambda: now.value)
    monkeypatch.setattr(kafka_source, "monotonic", lambda: 0.0)
    return now


def source(monkeypatch, fetches, apply):
    monkeypatch.setattr(clock_health, "require_clock_health", Mock(side_effect=AssertionError))
    monkeypatch.setattr(kafka_source, "apply_record", Mock(side_effect=apply))
    worker = SourceConsumer.__new__(SourceConsumer)
    worker.db = Database()
    worker.settings, worker.profile = SimpleNamespace(environment="test"), None
    worker.closing = False
    worker.client = Mock()
    worker.client.consume.side_effect = fetches
    worker.client.poll.return_value = None
    worker.retries = PartitionRetries(worker.client)
    worker.producers = {0: Mock(), 1: Mock()}
    return worker


def ahead(lead, at_offset):
    """Raise once for (0, at_offset); the retry finds the tenant clock past receipt."""
    raised = []

    def apply(db, *, partition, offset, **_):
        if partition == 0 and offset == at_offset and not raised:
            raised.append(offset)
            raise ReceiveTimeAhead(lead)
        return changed(partition, offset)

    return apply


def test_small_lead_holds_only_its_partition_for_the_lead_without_a_fault(
    monkeypatch, clock, caplog
):
    worker = source(
        monkeypatch,
        [[record(0, 20), record(0, 21), record(0, 22), record(1, 4)], [], [record(0, 21)]],
        ahead(0.25, 21),
    )
    with caplog.at_level(logging.INFO, logger=kafka_source.__name__):
        assert worker.turn() == 2
    # The prefix publishes; offset 22 is not applied ahead of the held input.
    applied = [
        (call.kwargs["partition"], call.kwargs["offset"])
        for call in kafka_source.apply_record.call_args_list
    ]
    assert applied == [(0, 20), (0, 21), (1, 4)]
    offsets = worker.producers[0].send_offsets_to_transaction.call_args.args[0]
    assert [(point.partition, point.offset) for point in offsets] == [(0, 21)]
    assert worker.client.seek.call_args.args[0].offset == 21
    point, deadline = worker.retries.failed[(TOPIC, 0)]
    assert point.offset == 21
    assert deadline == pytest.approx(100.0 + 0.25 + kafka_source.RECEIVE_LEAD_MARGIN)
    # Two bookkeeping transactions (one per partition) and no fault row.
    statements = [text for conn in worker.db.statements() for text, *_ in conn]
    assert not [text for text in statements if "kafka_pipeline_faults" in text]
    (entry,) = [r for r in caplog.records if r.msg.startswith("source_receive_time_ahead ")]
    assert entry.levelno == logging.WARNING  # above 100 ms
    assert entry.getMessage() == "source_receive_time_ahead partition=0 lead_seconds=0.250"

    # An idle consumer polls only until the held partition is due.
    clock.value = 100.2
    assert worker.turn() == 0
    assert worker.client.poll.call_args.args[0] == pytest.approx(0.06)
    clock.value = 100.27
    assert worker.turn() == 1
    assert (TOPIC, 0) not in worker.retries.failed
    worker.client.resume.assert_called_once()


def test_lead_under_100_ms_is_logged_at_info(monkeypatch, clock, caplog):
    worker = source(monkeypatch, [[record(0, 5)]], ahead(0.01, 5))
    with caplog.at_level(logging.INFO, logger=kafka_source.__name__):
        assert worker.turn() == 0
    (entry,) = [r for r in caplog.records if r.msg.startswith("source_receive_time_ahead ")]
    assert entry.levelno == logging.INFO
    assert worker.db.statements() == []
    assert worker.retries.failed[(TOPIC, 0)][1] == pytest.approx(100.02)


def test_lead_beyond_clock_health_bound_is_a_fault_with_the_normal_retry(monkeypatch, clock):
    worker = source(monkeypatch, [[record(0, 5)]], ahead(1.5, 5))
    assert worker.turn() == 0
    [fault] = worker.db.statements()
    assert "kafka_pipeline_faults" in fault[0][0]
    assert fault[0][1][:4] == (TOPIC, 0, 5, "source_receive_time_ahead")
    assert worker.retries.failed[(TOPIC, 0)][1] == pytest.approx(105.0)


def test_lead_at_the_bound_still_waits(monkeypatch, clock):
    worker = source(monkeypatch, [[record(0, 5)]], ahead(kafka_source.RECEIVE_LEAD_LIMIT, 5))
    assert worker.turn() == 0
    assert worker.db.statements() == []


def test_partition_retry_timeout_bounds_idle_polls(clock):
    retries = PartitionRetries(Mock())
    assert retries.timeout(0.1) == 0.1
    retries.fail(record(0, 1), delay=0.03)
    assert retries.timeout(0.1) == pytest.approx(0.03)
    retries.fail(record(1, 1))
    assert retries.failed[(TOPIC, 1)][1] == pytest.approx(105.0)  # default unchanged
    clock.value = 100.5
    assert retries.timeout(0.1) == 0.0
    retries.resume_due()
    assert retries.timeout(0.1) == 0.1
    retries.fence(SimpleNamespace(topic=TOPIC, partition=2))
    assert retries.timeout(0.1) == 0.1
