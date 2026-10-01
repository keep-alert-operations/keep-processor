"""Explicit offline protocol cutover; never starts or stops shared services."""

import argparse
import json

from psycopg.types.json import Jsonb

from appchi.cel import compile_expression
from appchi.config import Settings
from appchi.db import Database
from appchi.shared.errors import unavailable
from pathlib import Path


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


def activate(db, settings):
    from confluent_kafka import (
        Consumer,
        TopicPartition,
        ConsumerGroupState,
        KafkaException,
        KafkaError,
    )
    from confluent_kafka.admin import AdminClient
    from appchi.service_clients import kafka_options

    blocked = inspect_rules(db)
    with db.transaction() as conn:
        state = conn.execute("SELECT * FROM kafka_pipeline_control WHERE singleton").fetchone()
        if state["enabled"]:
            return state["cutover_offsets"]
        require_drained(conn)
    groups = {"source": "received", "correlation": "changed", "indexer": "changed"}
    identities = [f"appchi.{settings.environment}.{role}-v2" for role in groups]
    admin = AdminClient(kafka_options(settings))
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
    offsets = {}
    for role, stream in groups.items():
        client = Consumer(
            {
                **kafka_options(settings),
                "group.id": f"appchi.{settings.environment}.{role}-v2",
                "enable.auto.commit": False,
            }
        )
        try:
            topic = f"appchi.{settings.environment}.alerts.{stream}"
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
            "UPDATE kafka_pipeline_control SET enabled=true,cutover_at=clock_timestamp(),cutover_offsets=%s,received_partitions=%s,changed_partitions=%s WHERE singleton",
            (Jsonb(offsets), len(offsets["source"]), len(offsets["correlation"])),
        )
    return offsets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingress-and-legacy-workers-stopped", action="store_true", required=True)
    parser.add_argument(
        "--local-config",
        type=Path,
        help="Write development-only ingress routing after successful offline cutover",
    )
    args = parser.parse_args()
    if not args.ingress_and_legacy_workers_stopped:
        parser.error("Offline cutover assertion is required")
    settings = Settings()
    db = Database(
        settings.database_url.get_secret_value(), 1, deployment_epoch=settings.deployment_epoch
    )
    try:
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
