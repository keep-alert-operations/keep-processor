"""Kafka source transactions; PostgreSQL and Kafka are deliberately separate commits."""

from datetime import datetime
from hashlib import sha256
from uuid import UUID

from appchi.event_models import KafkaReceivedEnvelope
from appchi.ingestion.keys import received_key
from appchi.ingestion.native import identity, parse_json, validate_native
from appchi.shared.errors import AppError, unavailable
from appchi.shared.transactions import coordinator


def apply_record(db, *, topic, partition, offset, key, value, settings, profile):
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
    from appchi.alerts.processor import _apply_one
    from appchi.maintenance.runtime import prepare_for_tenant

    with db.transaction(tenant) as conn:
        old = conn.execute(
            "SELECT * FROM kafka_source_results WHERE delivery_id=%s", (envelope.event_id,)
        ).fetchone()
        if old:
            return replay_result(old, tenant, digest, topic, partition, offset)
    catalog = prepare_for_tenant(db, tenant, profile)
    with db.transaction(tenant) as conn:
        coordinator(conn, tenant)
        old = conn.execute(
            "SELECT * FROM kafka_source_results WHERE delivery_id=%s", (envelope.event_id,)
        ).fetchone()
        if old:
            return replay_result(old, tenant, digest, topic, partition, offset)
        operator = None
        if envelope.operator_id:
            operator = conn.execute(
                "SELECT deleted_at FROM operators WHERE id=%s AND tenant_id=%s",
                (envelope.operator_id, tenant),
            ).fetchone()
            if not operator:
                raise unavailable("source_operator_scope_invalid")
        order = conn.execute(
            "UPDATE tenant_work_state SET admission_order=admission_order+1 WHERE tenant_id=%s RETURNING admission_order",
            (tenant,),
        ).fetchone()["admission_order"]
        at = datetime.fromisoformat(envelope.received_at.replace("Z", "+00:00"))
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
                parse_json(result["envelope"])["event_id"] if result["envelope"] else None,
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

    On any uncertain transaction outcome discard this client and rejoin. A new
    init_transactions fences its predecessor and settles its transaction before
    the group offset is read. Never seek past an uncommitted database effect.
    """

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
                if low != 0:
                    raise unavailable("kafka_initial_retention_gap")
                committed = 0
            if committed < low:
                with self.db.transaction() as conn:
                    conn.execute(
                        "INSERT INTO kafka_pipeline_faults(consumer,topic,partition,input_offset,reason) VALUES('source',%s,%s,%s,'retention_gap') ON CONFLICT DO NOTHING",
                        (p.topic, p.partition, committed),
                    )
                raise unavailable("kafka_retention_gap")
            with self.db.transaction() as conn:
                conn.execute(
                    "UPDATE kafka_source_results SET kafka_confirmed=true WHERE topic=%s AND partition=%s AND input_offset<%s AND NOT kafka_confirmed",
                    (p.topic, p.partition, committed),
                )
            p.offset = committed
        consumer.assign(partitions)

    def _revoked(self, consumer, partitions):
        for p in partitions:
            self.producers.pop(p.partition, None)

    def turn(self):
        from confluent_kafka import TopicPartition
        from appchi.clock_health import require_clock_health

        require_clock_health(self.settings)

        message = self.client.poll(0.1)
        if message is None:
            return 0
        if message.error():
            raise unavailable("kafka_source_consume_failed")
        producer = self.producers[message.partition()]
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
        except (AppError, ValueError) as error:
            # Independent diagnostic transaction: do not roll this evidence back
            # with a failed domain mutation or persist arbitrary exception text.
            known = {
                "source_delivery_identity_conflict",
                "source_identity_proof_invalid",
                "source_topic_mismatch",
                "source_global_scope_mismatch",
                "source_operator_scope_invalid",
                "maintenance_proof_unavailable",
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
            raise
        producer.begin_transaction()
        if changed:
            body = parse_json(changed)
            producer.produce(
                f"appchi.{self.settings.environment}.alerts.changed",
                key=UUID(body["tenant_id"]).bytes,
                value=changed,
            )
        producer.send_offsets_to_transaction(
            [TopicPartition(message.topic(), message.partition(), message.offset() + 1)],
            self.client.consumer_group_metadata(),
            5,
        )
        producer.commit_transaction(10)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE kafka_source_results SET kafka_confirmed=true WHERE topic=%s AND partition=%s AND input_offset=%s",
                (message.topic(), message.partition(), message.offset()),
            )
            conn.execute(
                """INSERT INTO kafka_consumer_positions(consumer,topic,partition,next_offset) VALUES('source',%s,%s,%s)
                ON CONFLICT(consumer,topic,partition) DO UPDATE SET next_offset=greatest(kafka_consumer_positions.next_offset,EXCLUDED.next_offset),updated_at=clock_timestamp()""",
                (message.topic(), message.partition(), message.offset() + 1),
            )
        return 1

    def close(self):
        self.closing = True
        self.client.close()
        self.producers.clear()
