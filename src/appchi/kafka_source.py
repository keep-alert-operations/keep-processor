"""Kafka source transactions; PostgreSQL and Kafka are deliberately separate commits."""

from datetime import datetime
from hashlib import sha256
from time import monotonic
from uuid import UUID
import logging

from appchi.event_models import KafkaReceivedEnvelope
from appchi.ingestion.keys import received_key
from appchi.ingestion.native import identity, parse_json, validate_native
from appchi.shared.errors import AppError, unavailable
from appchi.shared.transactions import advance_clock, lock_coordinator
from appchi.kafka_changed import PartitionRetries

log = logging.getLogger(__name__)

# The clock-health contract bounds each host's offset at one second. A larger
# receipt lead is a clock fault; a smaller one is waited out without a fault.
RECEIVE_LEAD_LIMIT = 1.0
RECEIVE_LEAD_MARGIN = 0.01


class ReceiveTimeAhead(AppError):
    """Ingress stamped a receipt ahead of the tenant clock; retry once it passes."""

    def __init__(self, lead):
        super().__init__("source_receive_time_ahead", "Receipt is ahead of the tenant clock.", 503)
        self.lead = lead


class ChangedEvent(bytes):
    """Changed-event bytes carrying the input tenant that keys their publication.

    Replay proof binds a stored changed envelope to the same tenant, so the
    publisher need not parse the envelope again to find its key.
    """

    def __new__(cls, envelope, tenant):
        event = super().__new__(cls, envelope)
        event.tenant = tenant
        return event


def _keyed(tenant, changed):
    return None if changed is None else ChangedEvent(changed, tenant)


def apply_record(db, *, topic, partition, offset, key, value, settings, profile):
    """Apply one input; return its unconfirmed ``ChangedEvent`` or None."""
    envelope = KafkaReceivedEnvelope.model_validate(parse_json(value))
    tenant = UUID(envelope.tenant_id)
    if topic != f"appchi.{settings.environment}.alerts.received":
        raise unavailable("source_topic_mismatch")
    if envelope.native_version == "v1" and tenant != settings.global_tenant_id:
        raise unavailable("source_global_scope_mismatch")
    payload = validate_native(envelope.payload, envelope.native_version, envelope.raw_json.encode())
    logical_key, key_digest, material = identity(
        payload, envelope.native_version, envelope.operator_id
    )
    if (
        key != received_key(tenant, payload["application"], logical_key)
        or logical_key != envelope.key_field
        or material.decode() != envelope.identity_material
    ):
        raise unavailable("source_identity_proof_invalid")
    digest = sha256(value).digest()
    from appchi.maintenance.runtime import prepare_for_tenant

    at = datetime.fromisoformat(envelope.received_at.replace("Z", "+00:00"))
    application = dict(
        tenant=tenant,
        envelope=envelope,
        payload=payload,
        key_digest=key_digest,
        material=material,
        logical_key=logical_key,
        at=at,
        digest=digest,
        topic=topic,
        partition=partition,
        offset=offset,
    )
    with db.transaction(tenant) as conn:
        old = conn.execute(
            "SELECT * FROM kafka_source_results WHERE delivery_id=%s", (envelope.event_id,)
        ).fetchone()
        if old:
            return _keyed(tenant, replay_result(old, tenant, digest, topic, partition, offset))
        maintenance_gate = conn.execute(
            "SELECT ready FROM capability_gates WHERE capability='maintenance'"
        ).fetchone()
        if not maintenance_gate or not maintenance_gate["ready"]:
            # No native preparation is needed. Keep the already acquired restore
            # guard and transaction; the locked domain path still rechecks the
            # live gate/timeline and rolls back if the gate changed meanwhile.
            return _keyed(tenant, _apply_record_locked(conn, catalog=None, **application))
        claim = conn.execute(
            "SELECT alert_id FROM alert_key_claims WHERE tenant_id=%s AND schema_version=%s AND key_digest=%s",
            (tenant, envelope.native_version, key_digest),
        ).fetchone()
    catalog = prepare_for_tenant(
        db,
        tenant,
        profile,
        gate_ready=bool(maintenance_gate and maintenance_gate["ready"]),
        alert_id=claim["alert_id"] if claim else None,
        effective_at=at,
        source_item={
            "id": UUID(envelope.event_id),
            "tenant_id": tenant,
            "version": envelope.native_version,
            "key_field": logical_key,
            "operator_id": UUID(envelope.operator_id) if envelope.operator_id else None,
            "received_at": at,
            "payload": payload,
        },
    )
    with db.transaction(tenant) as conn:
        return _keyed(tenant, _apply_record_locked(conn, catalog=catalog, **application))


def _apply_record_locked(
    conn,
    *,
    tenant,
    envelope,
    payload,
    key_digest,
    material,
    logical_key,
    at,
    digest,
    topic,
    partition,
    offset,
    catalog,
):
    """Apply validated delivery and proof in the caller's guarded transaction."""
    from appchi.alerts.processor import _apply_one

    # One coordinator acquisition serves the whole source application. The
    # clock check precedes any write and, on a first application, also
    # allocates the admission order in the same statement.
    state = lock_coordinator(conn, tenant)
    old = conn.execute(
        "SELECT * FROM kafka_source_results WHERE delivery_id=%s", (envelope.event_id,)
    ).fetchone()
    if old:
        advance_clock(conn, tenant)
        return replay_result(old, tenant, digest, topic, partition, offset)
    now, order = advance_clock(conn, tenant, admission=True)
    if at > now:
        # Ingress stamps receipt with its own host clock. Applied now, a later
        # control boundary could precede this receipt and cover it on replay.
        # Roll back, including the clock and order, and wait out the lead.
        raise ReceiveTimeAhead((at - now).total_seconds())
    operator = None
    if envelope.operator_id:
        operator = conn.execute(
            "SELECT deleted_at FROM operators WHERE id=%s AND tenant_id=%s",
            (envelope.operator_id, tenant),
        ).fetchone()
        if not operator:
            # Rolls back the transaction, including the allocated order.
            raise unavailable("source_operator_scope_invalid")
    retired = conn.execute(
        "SELECT retired_at FROM alert_retired_keys WHERE tenant_id=%s AND schema_version=%s AND key_digest=%s",
        (tenant, envelope.native_version, key_digest),
    ).fetchone()
    if operator and operator["deleted_at"]:
        # A bounded acknowledged-delivery drain precedes deletion. A later
        # unconfirmed broker write must not resurrect a deleted operator's
        # owned resources; preserve its terminal operational outcome.
        result = {"alert_id": None, "error_code": "operator_deleted", "envelope": None}
    elif retired and at <= retired["retired_at"]:
        result = {"alert_id": None, "error_code": "retired_receipt", "envelope": None}
    else:
        result = _apply_one(
            conn,
            tenant,
            None,
            catalog,
            coordinated=(state, now),
            supplied_item={
                "id": UUID(envelope.event_id),
                "tenant_id": tenant,
                "operator_id": UUID(envelope.operator_id) if envelope.operator_id else None,
                "version": envelope.native_version,
                "key_field": logical_key,
                "key_digest": key_digest,
                "identity_material": material,
                "received_at": at,
                "admission_order": order,
                "payload": payload,
                "raw_json": envelope.raw_json.encode(),
                "time_defaulted": envelope.time_defaulted,
                "adapter_version": envelope.adapter_version,
            },
        )
    conn.execute(
        """INSERT INTO kafka_source_results(delivery_id,tenant_id,topic,partition,input_offset,input_digest,received_at,alert_id,outcome,error_code,changed_envelope,changed_event_id,application_order)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            envelope.event_id,
            tenant,
            topic,
            partition,
            offset,
            digest,
            at,
            result["alert_id"],
            "rejected" if result["error_code"] else "applied",
            result["error_code"],
            result["envelope"],
            result.get("event_id"),
            order,
        ),
    )
    return result["envelope"]


def replay_result(old, tenant, digest, topic, partition, offset):
    if bytes(old["input_digest"]) != digest or old["tenant_id"] != tenant:
        raise unavailable("source_delivery_identity_conflict")
    same_input = (old["topic"], old["partition"], old["input_offset"]) == (topic, partition, offset)
    return (
        bytes(old["changed_envelope"])
        if same_input and old["changed_envelope"] and not old["kafka_confirmed"]
        else None
    )


class SourceConsumer:
    """One stable transactional producer per owned input partition.

    Each turn applies a bounded batch per partition, one PostgreSQL proof
    transaction per input, then publishes the batch in one Kafka transaction.
    On any uncertain transaction outcome discard this client and rejoin. A new
    init_transactions fences its predecessor and settles its transaction before
    the group offset is read. Never seek past an uncommitted database effect.
    """

    # The pipeline's existing yield bound: at most 100 inputs per turn, and a
    # partition batch closes after 100 milliseconds of application.
    batch_records = 100
    batch_seconds = 0.1

    def __init__(self, db, settings, profile):
        from confluent_kafka import Consumer
        from appchi.service_clients import kafka_options

        self.db, self.settings, self.profile = db, settings, profile
        with db.transaction() as conn:
            self.control = conn.execute(
                "SELECT * FROM kafka_pipeline_control WHERE singleton"
            ).fetchone()
            if not self.control["enabled"]:
                raise unavailable("kafka_cutover_required")
        self.producers = {}
        self.closing = False
        self.client = Consumer(
            {
                **kafka_options(settings),
                "group.id": f"appchi.{settings.environment}.source-v2",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "isolation.level": "read_committed",
                "auto.offset.reset": "error",
                "partition.assignment.strategy": "range",
            }
        )
        self.retries = PartitionRetries(self.client)
        self.client.subscribe(
            [f"appchi.{settings.environment}.alerts.received"],
            on_assign=self._assigned,
            on_revoke=self._revoked,
        )

    def _assigned(self, consumer, partitions):
        if self.closing:
            return
        from confluent_kafka import Producer
        from appchi.service_clients import kafka_options

        metadata = consumer.list_topics(
            f"appchi.{self.settings.environment}.alerts.received", timeout=5
        )
        if (
            len(metadata.topics[f"appchi.{self.settings.environment}.alerts.received"].partitions)
            != self.control["received_partitions"]
        ):
            raise unavailable("kafka_partition_topology_changed")

        fenced = []
        for p in partitions:
            producer = Producer(
                {
                    **kafka_options(self.settings),
                    "transactional.id": f"appchi.{self.settings.environment}.source-v2.{p.partition}",
                    "enable.idempotence": True,
                    "acks": "all",
                    "transaction.timeout.ms": 30000,
                }
            )
            producer.init_transactions(10)
            self.producers[p.partition] = producer
            committed = consumer.committed([p], timeout=5)[0].offset
            low, _ = consumer.get_watermark_offsets(p, timeout=5)
            if committed < 0:
                committed = 0
            if committed < low:
                with self.db.transaction() as conn:
                    conn.execute(
                        "INSERT INTO kafka_pipeline_faults(consumer,topic,partition,input_offset,reason) VALUES('source',%s,%s,%s,'retention_gap') ON CONFLICT DO NOTHING",
                        (p.topic, p.partition, committed),
                    )
                p.offset = low
                fenced.append(p)
                continue
            with self.db.transaction() as conn:
                conn.execute(
                    "UPDATE kafka_source_results SET kafka_confirmed=true WHERE topic=%s AND partition=%s AND input_offset<%s AND NOT kafka_confirmed",
                    (p.topic, p.partition, committed),
                )
            p.offset = committed
        consumer.assign(partitions)
        for point in fenced:
            self.retries.fence(point)

    def _revoked(self, consumer, partitions):
        self.retries.revoke(partitions)
        for p in partitions:
            self.producers.pop(p.partition, None)

    def turn(self):
        # The worker loop checks clock health immediately before each turn.
        self.retries.resume_due()

        # Take already fetched inputs without waiting. Only an idle consumer
        # waits, and then for one input, so batching adds no idle latency.
        messages = self.client.consume(self.batch_records, timeout=0)
        if not messages:
            message = self.client.poll(self.retries.timeout(self.batch_seconds))
            messages = [] if message is None else [message]
        batches = {}
        for message in messages:
            if message.error():
                raise unavailable("kafka_source_consume_failed")
            batches.setdefault(message.partition(), []).append(message)
        progress = 0
        for partition, batch in batches.items():
            # A revoked partition belongs to its next owner; a retained or fenced
            # partition resumes only from its held offset. (A Producer defines
            # __len__, so test identity rather than truth.)
            producer = self.producers.get(partition)
            if producer is not None and (batch[0].topic(), partition) not in self.retries.failed:
                progress += self._apply_batch(producer, batch)
        return progress

    def _apply_batch(self, producer, batch):
        applied, progress, started = [], 0, monotonic()
        for message in batch:
            try:
                changed = apply_record(
                    self.db,
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key=message.key(),
                    value=message.value(),
                    settings=self.settings,
                    profile=self.profile,
                )
            except Exception as error:
                # Commit the healthy prefix before retaining this exact offset.
                progress += self._publish(producer, applied)
                self._retain(message, error)
                return progress
            applied.append((message, changed))
            if monotonic() - started >= self.batch_seconds:
                progress += self._publish(producer, applied)
                applied, started = [], monotonic()
        return progress + self._publish(producer, applied)

    def _publish(self, producer, applied):
        """Commit changed events in input order and the next offset atomically in Kafka."""
        from confluent_kafka import TopicPartition

        if not applied:
            return 0
        first, last = applied[0][0], applied[-1][0]
        # Every input is already committed in PostgreSQL, so the transaction
        # timeout covers only this publication.
        producer.begin_transaction()
        for _, changed in applied:
            if changed:
                producer.produce(
                    f"appchi.{self.settings.environment}.alerts.changed",
                    key=changed.tenant.bytes,
                    value=changed,
                )
        producer.send_offsets_to_transaction(
            [TopicPartition(last.topic(), last.partition(), last.offset() + 1)],
            self.client.consumer_group_metadata(),
            5,
        )
        producer.commit_transaction(10)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE kafka_source_results SET kafka_confirmed=true WHERE topic=%s AND partition=%s AND input_offset>=%s AND input_offset<%s AND NOT kafka_confirmed",
                (last.topic(), last.partition(), first.offset(), last.offset() + 1),
            )
            conn.execute(
                """INSERT INTO kafka_consumer_positions(consumer,topic,partition,next_offset) VALUES('source',%s,%s,%s)
                ON CONFLICT(consumer,topic,partition) DO UPDATE SET next_offset=greatest(kafka_consumer_positions.next_offset,EXCLUDED.next_offset),updated_at=clock_timestamp()""",
                (last.topic(), last.partition(), last.offset() + 1),
            )
        return len(applied)

    def _retain(self, message, error):
        if isinstance(error, ReceiveTimeAhead) and error.lead <= RECEIVE_LEAD_LIMIT:
            # Healthy clocks still differ: hold only this partition until the
            # tenant clock passes the receipt. Not a fault; warn above 100 ms.
            self.retries.fail(message, delay=error.lead + RECEIVE_LEAD_MARGIN)
            log.log(
                logging.WARNING if error.lead > 0.1 else logging.INFO,
                "source_receive_time_ahead",
                extra={"partition": message.partition(), "lead_seconds": error.lead},
            )
            return
        # Independent diagnostic transaction: do not roll this evidence back
        # with a failed domain mutation or persist arbitrary exception text.
        known = {
            "source_delivery_identity_conflict",
            "source_identity_proof_invalid",
            "source_topic_mismatch",
            "source_global_scope_mismatch",
            "source_operator_scope_invalid",
            "maintenance_proof_unavailable",
            "source_receive_time_ahead",
        }
        reason = (
            error.code
            if isinstance(error, AppError) and error.code in known
            else "source_processing_failed"
        )
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO kafka_pipeline_faults(consumer,topic,partition,input_offset,reason,digest) VALUES('source',%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    reason,
                    sha256(message.value()).digest(),
                ),
            )
        # No Kafka transaction covers this offset. Retain it and continue
        # processing unrelated assigned partitions during retry.
        self.retries.fail(message)
        log.warning(
            "source_partition_retry", extra={"partition": message.partition(), "reason": reason}
        )

    def close(self):
        self.closing = True
        self.client.close()
        self.producers.clear()
