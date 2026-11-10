"""Kafka source critical section: one coordinator, folded counter statements."""

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import SimpleNamespace
from uuid import uuid4

import pytest

from keep import kafka_source
from keep.shared.errors import AppError

BASE = datetime(2026, 10, 18, tzinfo=UTC)


class Result:
    def __init__(self, rows):
        self.rows = list(rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class SourceConn:
    """Minimal PostgreSQL stand-in for the statements this section issues."""

    def __init__(self, *, last_clock=None, clock=BASE, results=None, operator=None):
        self.work = {"tenant_id": None, "admission_order": 7, "last_clock": last_clock}
        self.clock = clock
        self.results = dict(results or {})
        self.operator = operator
        self.statements = []
        self.inserted = []

    def execute(self, query, params=()):
        text = " ".join(str(query).split())
        self.statements.append(text)
        if text.startswith("SELECT * FROM tenant_work_state") and "FOR UPDATE" in text:
            return Result([dict(self.work)])
        if "UPDATE tenant_work_state t SET last_clock=c.now" in text:
            last = self.work["last_clock"]
            if last is not None and self.clock < last:
                return Result([])
            self.work["last_clock"] = self.clock
            self.work["admission_order"] += params[0]
            return Result([{"now": self.clock, "admission_order": self.work["admission_order"]}])
        if text.startswith("SELECT * FROM kafka_source_results"):
            row = self.results.get(params[0])
            return Result([row] if row else [])
        if text.startswith("SELECT deleted_at FROM operators"):
            return Result([self.operator] if self.operator else [])
        if text.startswith("SELECT retired_at FROM alert_retired_keys"):
            return Result([])
        if text.startswith("INSERT INTO kafka_source_results"):
            self.inserted.append(params)
            return Result([])
        raise AssertionError(f"unexpected statement: {text}")

    def work_statements(self):
        return [text for text in self.statements if "tenant_work_state" in text]


def application(tenant, event_id=None, operator_id=None):
    envelope = SimpleNamespace(
        event_id=str(event_id or uuid4()),
        operator_id=operator_id,
        native_version="v1",
        raw_json="{}",
        time_defaulted=False,
        adapter_version=None,
    )
    return dict(
        tenant=tenant,
        envelope=envelope,
        payload={"application": "a"},
        key_digest=b"k",
        material=b"m",
        logical_key="key",
        at=BASE - timedelta(seconds=1),
        digest=sha256(b"value").digest(),
        topic="keep.test.alerts.received",
        partition=0,
        offset=3,
        catalog=None,
    )


@pytest.fixture
def applied(monkeypatch):
    calls = []

    def fake_apply_one(conn, tenant, attempt, catalog, *, supplied_item=None, coordinated=None):
        calls.append({"supplied_item": supplied_item, "coordinated": coordinated})
        return {"alert_id": uuid4(), "error_code": None, "envelope": b"{}", "event_id": "evt-1"}

    monkeypatch.setattr("keep.alerts.processor._apply_one", fake_apply_one)
    return calls


def test_first_application_takes_the_coordinator_once(applied):
    tenant = uuid4()
    conn = SourceConn(last_clock=BASE - timedelta(seconds=5))
    assert kafka_source._apply_record_locked(conn, **application(tenant)) == b"{}"

    # Lock plus one clock/admission statement; previously six statements here
    # (coordinator twice) and a separate admission_order UPDATE.
    assert len(conn.work_statements()) == 2
    assert conn.work_statements()[0].endswith("FOR UPDATE")
    assert not [text for text in conn.statements if text.startswith("SELECT clock_timestamp()")]
    (call,) = applied
    state, now = call["coordinated"]
    assert state["admission_order"] == 7 and now == BASE
    assert call["supplied_item"]["admission_order"] == 8
    assert conn.work == {"tenant_id": None, "admission_order": 8, "last_clock": BASE}
    (row,) = conn.inserted
    assert row[11] == "evt-1"  # changed_event_id comes from publish, not a re-parse
    assert row[12] == 8  # application_order is the allocated admission order


def test_replay_returns_retained_result_without_allocating_order(applied):
    tenant = uuid4()
    event_id = uuid4()
    args = application(tenant, event_id)
    retained = {
        "input_digest": args["digest"],
        "tenant_id": tenant,
        "topic": args["topic"],
        "partition": args["partition"],
        "input_offset": args["offset"],
        "changed_envelope": b"retained",
        "kafka_confirmed": False,
    }
    conn = SourceConn(results={str(event_id): retained})
    assert kafka_source._apply_record_locked(conn, **args) == b"retained"
    assert applied == []
    assert conn.inserted == []
    assert conn.work["admission_order"] == 7
    assert conn.work["last_clock"] == BASE
    assert len(conn.work_statements()) == 2


@pytest.mark.parametrize("replay", [False, True])
def test_clock_regression_is_rejected_before_any_write(applied, replay):
    tenant = uuid4()
    event_id = uuid4()
    args = application(tenant, event_id)
    results = (
        {str(event_id): {"input_digest": args["digest"], "tenant_id": tenant}} if replay else {}
    )
    conn = SourceConn(last_clock=BASE + timedelta(seconds=1), results=results)
    with pytest.raises(AppError) as raised:
        kafka_source._apply_record_locked(conn, **args)
    assert raised.value.code == "clock_untrusted"
    assert applied == [] and conn.inserted == []
    assert conn.work["admission_order"] == 7
    assert conn.work["last_clock"] == BASE + timedelta(seconds=1)


def test_unknown_operator_still_fails_closed(applied):
    conn = SourceConn()
    with pytest.raises(AppError) as raised:
        kafka_source._apply_record_locked(conn, **application(uuid4(), operator_id=str(uuid4())))
    assert raised.value.code == "source_operator_scope_invalid"
    assert applied == [] and conn.inserted == []
