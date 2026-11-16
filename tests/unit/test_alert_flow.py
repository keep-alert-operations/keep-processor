"""The steps of one delivered input inside the source processor, as log records."""

from contextlib import contextmanager
from datetime import UTC, datetime
import json
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from keep import kafka_source
from keep.event_models import KafkaReceivedEnvelope
from keep.ingestion.keys import received_key
from keep.ingestion.native import canonical, identity, validate_native
from keep.kafka_source import SourceConsumer
from keep.observability import alert_fields, flow as alert_flow
from keep.observability.testing import capture_records
from keep.serialization import wire
from keep.shared.errors import unavailable

TENANT = uuid4()
TOPIC = "keep.test.alerts.received"
SETTINGS = SimpleNamespace(environment="test", global_tenant_id=TENANT)
V1 = {
    "application": "billing",
    "object": "database",
    "node_name": "db-1",
    "message": "disk is full",
    "operator": "legacy",
    "time_created": "01-01-2026 00:00:00",
    "severity": "error",
}


def delivered(offset=7):
    """One accepted v1 input as ingestion publishes it, as a Kafka message."""
    raw = json.dumps(V1).encode()
    payload = validate_native(V1, "v1", raw)
    key, _, material = identity(payload, "v1", None)
    delivery = str(uuid4())
    body = KafkaReceivedEnvelope.model_validate(
        {
            "schema_version": 2,
            "kind": "alerts.received",
            "event_id": delivery,
            "tenant_id": str(TENANT),
            "operator_id": None,
            "credential_id": None,
            "policy_generation": None,
            "native_version": "v1",
            "received_at": wire(datetime.now(UTC)),
            "payload": payload,
            "raw_json": raw.decode(),
            "time_defaulted": False,
            "key_field": key,
            "identity_material": material.decode(),
            "adapter_version": None,
        }
    )
    value = canonical(body.model_dump())
    message = Mock(
        topic=lambda: TOPIC,
        partition=lambda: 2,
        offset=lambda: offset,
        key=lambda: received_key(TENANT, V1["application"], key),
        value=lambda: value,
    )
    return delivery, message


class Database:
    """Answers the two lookups ``apply_record`` makes before applying."""

    def __init__(self, old=None, claim=None):
        self.old, self.claim = old, claim

    @contextmanager
    def transaction(self, *args, **kwargs):
        conn = Mock()

        def execute(text, *_):
            row = self.old if "kafka_source_results WHERE delivery_id" in text else None
            if "alert_key_claims" in text:
                row = self.claim
            return Mock(fetchone=lambda: row)

        conn.execute.side_effect = execute
        yield conn


@pytest.fixture
def records():
    return capture_records("processor", "source")


@pytest.fixture
def worker(monkeypatch):
    monkeypatch.setattr("keep.maintenance.runtime.prepare_for_tenant", lambda *a, **k: None)
    consumer = SourceConsumer.__new__(SourceConsumer)
    consumer.db = Database()
    consumer.settings, consumer.profile = SETTINGS, None
    consumer.client, consumer.retries = Mock(), Mock()
    return consumer


def applying(monkeypatch, alert_id, changed_event, *, fail=None):
    """Stand in for the locked application: note the outcome as the shared code does."""

    def locked(conn, *, payload, **_):
        alert_flow.note(
            "alert_applied",
            learned={"alert": {"id": alert_id}, "changed_event_id": changed_event},
            alert=alert_fields(payload, whole=True),
            outcome="created",
            status_before="none",
            status_after="firing",
            revision=1,
            suppressed=False,
            projection_seq=41,
        )
        if fail:
            raise fail
        return b'{"changed": true}'

    monkeypatch.setattr(kafka_source, "_apply_record_locked", locked)


def actions(records):
    return [(r["event"]["sequence"], r["event"]["action"]) for r in records]


def test_an_applied_delivery_leaves_its_four_steps_in_order(records, worker, monkeypatch):
    """Requirements: REV-74; observability 3."""
    delivery, message = delivered()
    applying(monkeypatch, "alert-1", "change-1")
    assert worker._apply_batch(Mock(), [message]) == 1
    consumed, matched, applied, published = records()
    assert actions([consumed, matched, applied, published]) == [
        (1, "alert_consumed"),
        (2, "alert_matched"),
        (3, "alert_applied"),
        (4, "alert_change_published"),
    ]
    for record in (consumed, matched, applied, published):
        keep = record["keep"]
        assert keep["delivery_id"] == delivery
        assert keep["kafka"] == {"partition": 2, "offset": 7}
        assert len(keep["alert"]["key_digest"]) == 64
        assert (keep["alert"]["application"], keep["alert"]["object"]) == ("billing", "database")
        assert record["service"]["name"] == "processor"
    assert matched["keep"]["outcome"] == "new_alert"
    # The alert and its change exist only once the application committed.
    assert "id" not in consumed["keep"]["alert"] and "id" not in matched["keep"]["alert"]
    for record in (applied, published):
        assert record["keep"]["alert"]["id"] == "alert-1"
        assert record["keep"]["changed_event_id"] == "change-1"
    assert applied["keep"]["alert"]["message"] == "disk is full"
    assert "message" not in published["keep"]["alert"]
    assert applied["message"] == "Alert created: status none to firing, revision 1"
    assert consumed["keep"]["delay_seconds"] >= 0


def test_an_existing_alert_is_named_from_the_identity_lookup_onward(records, worker, monkeypatch):
    """Requirements: REV-74; observability 3."""
    _, message = delivered()
    worker.db = Database(claim={"alert_id": "alert-9"})
    applying(monkeypatch, "alert-9", "change-2")
    worker._apply_batch(Mock(), [message])
    matched = records()[1]
    assert matched["keep"]["outcome"] == "existing_alert"
    assert matched["keep"]["alert"]["id"] == "alert-9"


def test_a_rolled_back_application_leaves_no_applied_record(records, worker, monkeypatch):
    """Requirements: REV-74; observability 3 and 5. One record for the failure."""
    delivery, message = delivered()
    applying(monkeypatch, "alert-1", "change-1", fail=unavailable("maintenance_proof_unavailable"))
    assert worker._apply_batch(Mock(), [message]) == 0
    assert [r["event"]["action"] for r in records()] == [
        "alert_consumed",
        "alert_matched",
        "source_partition_retry",
    ]
    retained = records()[-1]
    assert retained["event"]["sequence"] == 3
    assert retained["log"]["level"] == "warning"
    assert retained["keep"]["delivery_id"] == delivery
    assert retained["keep"]["reason"] == "maintenance_proof_unavailable"
    assert retained["error"]["code"] == "maintenance_proof_unavailable"
    assert retained["keep"]["alert"]["application"] == "billing"
    # The alert was never created: its rolled-back ID must not appear.
    assert "id" not in retained["keep"]["alert"]


@pytest.mark.parametrize(("offset", "outcome"), [(7, "replayed"), (9, "duplicate")])
def test_a_delivery_with_a_stored_result_is_recorded_as_reused(
    records, worker, monkeypatch, offset, outcome
):
    """Requirements: REV-74; observability 3."""
    delivery, message = delivered(offset)
    from hashlib import sha256

    worker.db = Database(
        old={
            "delivery_id": delivery,
            "tenant_id": TENANT,
            "topic": TOPIC,
            "partition": 2,
            "input_offset": 7,
            "input_digest": sha256(message.value()).digest(),
            "alert_id": "alert-3",
            "changed_envelope": None,
            "changed_event_id": "change-3",
            "kafka_confirmed": True,
            "error_code": None,
        }
    )
    assert worker._apply_batch(Mock(), [message]) == 1
    consumed, replayed, committed = records()
    assert actions([consumed, replayed, committed]) == [
        (1, "alert_consumed"),
        (2, "alert_replayed"),
        (3, "alert_input_committed"),
    ]
    assert replayed["keep"]["outcome"] == outcome
    assert replayed["keep"]["alert"]["id"] == "alert-3"
    assert committed["keep"]["changed_event_id"] == "change-3"


def test_outcome_only_setting_keeps_the_applied_and_published_records(
    records, worker, monkeypatch
):
    """Requirements: REV-74; observability 3 and 10."""
    from keep.observability import logs

    monkeypatch.setattr(logs, "DETAIL", False)
    _, message = delivered()
    applying(monkeypatch, "alert-1", "change-1")
    worker._apply_batch(Mock(), [message])
    assert [r["event"]["action"] for r in records()] == ["alert_applied", "alert_change_published"]
