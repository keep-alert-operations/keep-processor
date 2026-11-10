"""Folded coordinator statements against isolated real PostgreSQL (no broker).

Run with KEEP_TEST_DATABASE_URL pointing at a disposable database.
"""

from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import psycopg
import pytest

from keep import kafka_source
from keep.db import BudgetConnection
from keep.event_models import KafkaReceivedEnvelope
from keep.ingestion.keys import received_key
from keep.ingestion.native import canonical, identity, validate_native
from keep.request_budget import request_budget
from keep.serialization import wire
from keep.shared.errors import AppError
from keep.shared.transactions import coordinator

pytestmark = pytest.mark.integration


class Counting:
    """Record statements issued through one locked source application."""

    def __init__(self, conn, log):
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_log", log)

    def execute(self, query, *args, **kwargs):
        raw = self._conn.connection if isinstance(self._conn, BudgetConnection) else self._conn
        text = query.as_string(raw) if hasattr(query, "as_string") else str(query)
        self._log.append(" ".join(text.split()))
        return self._conn.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __setattr__(self, name, value):
        # Inline-effect bindings must land on the connection db.py flushes.
        setattr(self._conn, name, value)


@pytest.fixture
def counted(monkeypatch):
    runs = []
    original = kafka_source._apply_record_locked

    def wrapper(conn, **kwargs):
        runs.append([])
        return original(Counting(conn, runs[-1]), **kwargs)

    monkeypatch.setattr(kafka_source, "_apply_record_locked", wrapper)
    return runs


@pytest.fixture
def context_relocks(monkeypatch):
    from keep.incidents import state

    calls = []
    original = state.lock_contexts

    def spy(*args, **kwargs):
        calls.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(state, "lock_contexts", spy)
    return calls


def settings(db):
    with db.transaction() as conn:
        tenant = conn.execute("SELECT id FROM tenants WHERE kind='global'").fetchone()["id"]
    return SimpleNamespace(environment="test-" + uuid4().hex, global_tenant_id=tenant)


def record(config, payload, at=None):
    at = at or datetime.now(UTC) - timedelta(seconds=1)
    original = canonical(payload)
    payload = validate_native(payload, "v1", original)
    key, _, material = identity(payload, "v1", None)
    body = KafkaReceivedEnvelope.model_validate(
        {
            "schema_version": 2,
            "kind": "alerts.received",
            "event_id": str(uuid4()),
            "tenant_id": str(config.global_tenant_id),
            "operator_id": None,
            "credential_id": None,
            "policy_generation": None,
            "native_version": "v1",
            "received_at": wire(at),
            "payload": payload,
            "raw_json": original.decode(),
            "time_defaulted": False,
            "key_field": key,
            "identity_material": material.decode(),
            "adapter_version": None,
        }
    )
    return (
        f"keep.{config.environment}.alerts.received",
        received_key(config.global_tenant_id, payload["application"], key),
        canonical(body.model_dump()),
    )


def apply(db, config, message, offset):
    topic, key, value = message
    return kafka_source.apply_record(
        db,
        topic=topic,
        partition=0,
        offset=offset,
        key=key,
        value=value,
        settings=config,
        profile=None,
    )


def work(db, tenant):
    with db.transaction(tenant) as conn:
        return conn.execute(
            "SELECT admission_order,change_order,projection_seq,last_clock FROM tenant_work_state WHERE tenant_id=%s",
            (tenant,),
        ).fetchone()


def count(log, fragment):
    return len([text for text in log if fragment in text])


def test_source_application_folds_counters_and_keeps_results(real_db, v1, counted, context_relocks):
    config = settings(real_db)
    tenant = config.global_tenant_id
    first = record(config, v1)
    before = work(real_db, tenant)
    with request_budget(30):  # publish pipelines through BudgetConnection
        envelope = apply(real_db, config, first, 0)
    after = work(real_db, tenant)
    (log,) = counted
    assert count(log, "FROM tenant_work_state WHERE tenant_id=%s FOR UPDATE") == 1
    assert count(log, "UPDATE tenant_work_state t SET last_clock=c.now") == 1
    assert count(log, "UPDATE tenant_work_state SET change_order") == 1
    assert count(log, "UPDATE tenant_work_state SET admission_order") == 0
    assert count(log, "UPDATE tenant_work_state SET last_clock") == 0
    assert count(log, "INSERT INTO alerts ") == 1 and count(log, "UPDATE alerts SET") == 0
    assert count(log, "UPDATE alert_source_events") == 0
    assert after["admission_order"] == before["admission_order"] + 1
    assert after["change_order"] == before["change_order"] + 1
    assert after["projection_seq"] == before["projection_seq"] + 1
    assert before["last_clock"] is None or after["last_clock"] > before["last_clock"]
    changed = json.loads(envelope)
    # The publication key travels with the result; it is the envelope's tenant.
    assert envelope.tenant == UUID(str(tenant)) == UUID(changed["tenant_id"])
    assert changed["projection_seq"] == str(after["projection_seq"])
    assert changed["original_admission_order"] == str(after["admission_order"])
    delivery = json.loads(first[2])["event_id"]
    with real_db.transaction(tenant) as conn:
        result = conn.execute(
            "SELECT * FROM kafka_source_results WHERE delivery_id=%s", (delivery,)
        ).fetchone()
        alert = conn.execute(
            "SELECT revision,projection_version FROM alerts WHERE id=%s", (changed["alert_id"],)
        ).fetchone()
        projection = conn.execute(
            "SELECT snapshot FROM projection_work WHERE tenant_id=%s AND seq=%s",
            (tenant, after["projection_seq"]),
        ).fetchone()
        source = conn.execute(
            "SELECT adapter_version FROM alert_source_events WHERE id=%s", (delivery,)
        ).fetchone()
    assert result["outcome"] == "applied"
    assert str(result["changed_event_id"]) == changed["event_id"]
    assert result["application_order"] == after["admission_order"]
    assert bytes(result["changed_envelope"]) == envelope
    assert (alert["revision"], alert["projection_version"]) == (1, 1)
    assert projection["snapshot"]["notes"] == [] and projection["snapshot"]["live"] is True
    assert source["adapter_version"] is None

    # Same delivery and offset: the unlocked precheck returns the retained
    # result; the locked replay branch is covered by the unit tests.
    replay = apply(real_db, config, first, 0)
    assert replay == envelope and replay.tenant == envelope.tenant
    assert len(counted) == 1
    replayed = work(real_db, tenant)
    assert replayed == after

    # Existing alert: lock_catalog's context lock is not repeated.
    repeated = json.loads(apply(real_db, config, record(config, v1), 1))
    final = work(real_db, tenant)
    assert final["admission_order"] == replayed["admission_order"] + 1
    assert final["change_order"] == replayed["change_order"]  # unchanged repeat
    assert int(repeated["projection_version"]) == int(changed["projection_version"]) + 1
    assert context_relocks == []
    assert count(counted[1], "INSERT INTO alerts ") == 0
    assert count(counted[1], "UPDATE alerts SET") == 1


def test_coordinator_clock_check_and_lock_strength(real_db, tenant):
    with real_db.transaction(tenant) as conn:
        state, first = coordinator(conn, tenant)
        assert state["last_clock"] is None
        _, second = coordinator(conn, tenant)
        assert second >= first
        stored = conn.execute(
            "SELECT last_clock FROM tenant_work_state WHERE tenant_id=%s", (tenant,)
        ).fetchone()["last_clock"]
        assert stored == second
        # FOR UPDATE (not NO KEY UPDATE) still excludes key-share lockers.
        with pytest.raises(psycopg.errors.LockNotAvailable):
            with real_db.transaction(tenant) as other:
                other.execute(
                    "SELECT 1 FROM tenant_work_state WHERE tenant_id=%s FOR KEY SHARE NOWAIT",
                    (tenant,),
                )
    future = datetime.now(UTC) + timedelta(hours=1)
    with real_db.transaction(tenant) as conn:
        conn.execute(
            "UPDATE tenant_work_state SET last_clock=%s WHERE tenant_id=%s", (future, tenant)
        )
        before = conn.execute(
            "SELECT * FROM tenant_work_state WHERE tenant_id=%s", (tenant,)
        ).fetchone()
        with pytest.raises(AppError) as raised:
            coordinator(conn, tenant)
        assert raised.value.code == "clock_untrusted"
        unchanged = conn.execute(
            "SELECT * FROM tenant_work_state WHERE tenant_id=%s", (tenant,)
        ).fetchone()
        assert unchanged == before
