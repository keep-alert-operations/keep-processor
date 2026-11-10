"""Explicit offline protocol cutover; never starts or stops shared services."""

import argparse
import json
from hashlib import sha256
from uuid import uuid4

from psycopg.types.json import Jsonb

from keep.cel import compile_expression
from keep.config import Settings
from keep.db import Database
from keep.kafka_source import same_topic, topic_identity
from keep.shared.errors import unavailable
from pathlib import Path

STREAMS = ("received", "changed")
GROUPS = {"source": "received", "correlation": "changed", "indexer": "changed"}


def inspect_rules(db):
    with db.transaction() as conn:
        rows = conn.execute(
            "SELECT r.id,r.tenant_id,r.revision,r.group_by,c.cel FROM correlation_rules r LEFT JOIN correlation_conditions c ON c.rule_id=r.id AND c.tenant_id=r.tenant_id WHERE r.deleted_at IS NULL"
        ).fetchall()
    blocked = set()
    for row in rows:
        if "assignee_id" in row["group_by"] or (
            row["cel"] and "assignee_id" in compile_expression(row["cel"], rule=True).fields
        ):
            blocked.add((row["tenant_id"], row["id"], row["revision"]))
    return blocked


def require_drained(conn):
    checks = (
        "SELECT 1 FROM ingest_items WHERE disposition='pending' LIMIT 1",
        "SELECT 1 FROM application_outbox WHERE stream IN ('received','changed') AND published_at IS NULL LIMIT 1",
        "SELECT 1 FROM projection_work WHERE NOT visible LIMIT 1",
        "SELECT 1 FROM maintenance_effect_outbox WHERE processed_at IS NULL LIMIT 1",
        "SELECT 1 FROM correlation_consumer_state c JOIN tenant_work_state w USING(tenant_id) WHERE c.change_order<w.change_order OR c.pending LIMIT 1",
    )
    if any(conn.execute(query).fetchone() for query in checks):
        raise unavailable("legacy_pipeline_not_drained")


def require_groups_stopped(admin, settings):
    from confluent_kafka import ConsumerGroupState, KafkaException, KafkaError

    identities = [f"keep.{settings.environment}.{role}-v2" for role in GROUPS]
    for future in admin.describe_consumer_groups(identities, request_timeout=5).values():
        try:
            description = future.result(6)
        except KafkaException as error:
            if error.args[0].code() == KafkaError.GROUP_ID_NOT_FOUND:
                continue
            raise unavailable("cutover_group_state_unverified") from None
        if description.members or description.state not in (
            ConsumerGroupState.EMPTY,
            ConsumerGroupState.DEAD,
        ):
            raise unavailable("cutover_consumers_active")


def activate(db, settings):
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient
    from keep.service_clients import kafka_options

    blocked = inspect_rules(db)
    with db.transaction() as conn:
        state = conn.execute("SELECT * FROM kafka_pipeline_control WHERE singleton").fetchone()
        if state["enabled"]:
            return state["cutover_offsets"]
        require_drained(conn)
    admin = AdminClient(kafka_options(settings))
    require_groups_stopped(admin, settings)
    # The source refuses any later topic incarnation (a recreated topic
    # restarts its offsets under the replay proof recorded from this one).
    topic_ids = {
        stream: topic_identity(admin, f"keep.{settings.environment}.alerts.{stream}")
        for stream in STREAMS
    }
    offsets = {}
    for role, stream in GROUPS.items():
        client = Consumer(
            {
                **kafka_options(settings),
                "group.id": f"keep.{settings.environment}.{role}-v2",
                "enable.auto.commit": False,
            }
        )
        try:
            topic = f"keep.{settings.environment}.alerts.{stream}"
            metadata = client.list_topics(topic, timeout=5).topics[topic]
            if metadata.error:
                raise unavailable("cutover_topic_unavailable")
            boundaries = []
            for partition in sorted(metadata.partitions):
                _, high = client.get_watermark_offsets(TopicPartition(topic, partition), timeout=5)
                boundaries.append(TopicPartition(topic, partition, high))
            # This command requires all ingress and incompatible workers stopped.
            # Repeating a partial cutover is safe because no new receipts exist yet.
            client.commit(offsets=boundaries, asynchronous=False)
            offsets[role] = [
                {"topic": p.topic, "partition": p.partition, "next_offset": p.offset}
                for p in boundaries
            ]
        finally:
            client.close()
    with db.transaction() as conn:
        conn.execute("SELECT singleton FROM kafka_pipeline_control WHERE singleton FOR UPDATE")
        require_drained(conn)
        for tenant, rule, revision in blocked:
            updated = conn.execute(
                "UPDATE correlation_rules SET kafka_schema_blocked=true,health='blocked',blocked_reason='correlation_field_removed' WHERE tenant_id=%s AND id=%s AND revision=%s RETURNING id",
                (tenant, rule, revision),
            ).fetchone()
            if not updated:
                raise unavailable("cutover_rule_changed")
            conn.execute(
                "DELETE FROM correlation_evidence WHERE tenant_id=%s AND rule_id=%s", (tenant, rule)
            )
        conn.execute(
            "UPDATE kafka_pipeline_control SET enabled=true,cutover_at=clock_timestamp(),cutover_offsets=%s,received_partitions=%s,changed_partitions=%s,received_topic_id=%s,changed_topic_id=%s WHERE singleton",
            (
                Jsonb(offsets),
                len(offsets["source"]),
                len(offsets["correlation"]),
                topic_ids["received"],
                topic_ids["changed"],
            ),
        )
    return offsets


def recorded_history(db, topics):
    """PostgreSQL's evidence per partition: highest observed next offset, last source result."""
    with db.transaction() as conn:
        positions = {
            (row["topic"], row["partition"]): row["next_offset"]
            for row in conn.execute(
                "SELECT topic,partition,max(next_offset) AS next_offset FROM kafka_consumer_positions WHERE topic=ANY(%s) GROUP BY topic,partition",
                (list(topics),),
            )
        }
        results = {
            (row["topic"], row["partition"]): (row["input_offset"], bytes(row["input_digest"]))
            for row in conn.execute(
                """SELECT DISTINCT ON (partition) topic,partition,input_offset,input_digest
                FROM kafka_source_results WHERE topic=%s ORDER BY partition,input_offset DESC""",
                (topics[0],),
            )
        }
    return positions, results


def record_digest(consumer, topic, partition, offset):
    """Read one retained record by assignment, never committing; None if absent."""
    from confluent_kafka import TopicPartition

    consumer.assign([TopicPartition(topic, partition, offset)])
    try:
        for _ in range(10):
            message = consumer.poll(1.0)
            if message is not None and not message.error():
                return sha256(message.value()).digest() if message.offset() == offset else None
        return None
    finally:
        consumer.unassign()


def require_history(consumer, topics, partition_counts, positions, results):
    """Refuse unless each broker partition still holds the history PostgreSQL recorded.

    A recreated topic restarts its offsets: it either ends below a recorded
    position or source result, or holds another record at the last source result.
    """
    from confluent_kafka import TopicPartition

    metadata = consumer.list_topics(timeout=5)
    for topic, count in zip(topics, partition_counts):
        described = metadata.topics.get(topic)
        if described is None or described.error or len(described.partitions) != count:
            raise unavailable("kafka_partition_topology_changed")
        for partition in sorted(described.partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=5)
            key = (topic, partition)
            result = results.get(key)
            if high < max(positions.get(key, 0), result[0] + 1 if result else 0):
                raise unavailable("kafka_topic_history_mismatch")
            if result and result[0] >= low:
                if record_digest(consumer, topic, partition, result[0]) != result[1]:
                    raise unavailable("kafka_topic_history_mismatch")


def record_topic_identity(db, settings):
    """Record the topic IDs of an existing cutover after verifying broker history.

    For a cutover recorded before migration 0054, and as the last step of a
    reviewed reset once stale source results are relabelled. Source, correlation
    and indexer groups must have no members. Returns previous and recorded IDs.
    """
    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient
    from keep.service_clients import kafka_options

    with db.transaction() as conn:
        state = conn.execute("SELECT * FROM kafka_pipeline_control WHERE singleton").fetchone()
    if not state["enabled"]:
        raise unavailable("kafka_cutover_required")
    if "received_topic_id" not in state:
        raise unavailable("kafka_topic_identity_migration_required")
    admin = AdminClient(kafka_options(settings))
    require_groups_stopped(admin, settings)
    topics =[f"keep.{settings.environment}.alerts.{stream}" for stream in STREAMS]
    observed = dict(zip(STREAMS, (topic_identity(admin, topic) for topic in topics)))
    previous = {stream: state[f"{stream}_topic_id"] for stream in STREAMS}
    if all(previous[s] and same_topic(previous[s], observed[s]) for s in STREAMS):
        return {"previous": previous, "recorded": previous}
    positions, results = recorded_history(db, topics)
    consumer = Consumer(
        {
            **kafka_options(settings),
            "group.id": f"keep.{settings.environment}.topic-identity-{uuid4()}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    try:
        require_history(
            consumer,
            topics,
            (state["received_partitions"], state["changed_partitions"]),
            positions,
            results,
        )
    finally:
        consumer.close()
    with db.transaction() as conn:
        updated = conn.execute(
            "UPDATE kafka_pipeline_control SET received_topic_id=%s,changed_topic_id=%s WHERE singleton AND enabled AND received_topic_id IS NOT DISTINCT FROM %s AND changed_topic_id IS NOT DISTINCT FROM %s RETURNING singleton",
            (observed["received"], observed["changed"], previous["received"], previous["changed"]),
        ).fetchone()
        if not updated:
            raise unavailable("kafka_pipeline_control_changed")
    return {"previous": previous, "recorded": observed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingress-and-legacy-workers-stopped", action="store_true")
    parser.add_argument(
        "--local-config",
        type=Path,
        help="Write development-only ingress routing after successful offline cutover",
    )
    parser.add_argument(
        "--record-topic-identity",
        action="store_true",
        help="After a recorded cutover or a reviewed reset, verify that the topics still hold "
        "PostgreSQL's history and record their topic IDs; stop source, correlation and indexer",
    )
    args = parser.parse_args()
    if args.record_topic_identity and args.local_config:
        parser.error("--local-config applies only to the offline cutover")
    if not (args.ingress_and_legacy_workers_stopped or args.record_topic_identity):
        parser.error("Offline cutover assertion is required")
    settings = Settings()
    db = Database(
        settings.database_url.get_secret_value(), 1, deployment_epoch=settings.deployment_epoch
    )
    try:
        if args.record_topic_identity:
            print(json.dumps({"topic_identity": record_topic_identity(db, settings)}))
            return
        result = activate(db, settings)
        if args.local_config:
            if settings.environment != "development":
                raise ValueError("Local output is development only")
            with db.transaction() as conn:
                tenant = conn.execute("SELECT id FROM tenants WHERE kind='global'").fetchone()["id"]
            args.local_config.write_text(
                json.dumps({"enabled": True, "global_tenant_id": str(tenant)}, indent=2) + "\n",
                encoding="utf-8",
            )
        print(json.dumps({"cutover": result}))
    finally:
        db.close()


if __name__ == "__main__":
    main()
