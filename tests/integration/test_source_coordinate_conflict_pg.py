"""Another delivery at a recorded source coordinate, against isolated real PostgreSQL.

Run with APPCHI_TEST_DATABASE_URL pointing at a disposable database.
"""

import json

import pytest

from appchi.shared.errors import AppError
from test_source_critical_section_pg import apply, record, settings, work

pytestmark = pytest.mark.integration


def test_another_delivery_at_a_recorded_coordinate_is_a_conflict(real_db, v1):
    """Requirements: REV-41; Kafka alert pipeline 3, 8.

    A recreated topic restarts its offsets, so a new delivery arrives at a
    coordinate whose replay proof belongs to the lost incarnation.
    """
    config = settings(real_db)
    tenant = config.global_tenant_id
    first = record(config, v1)
    original = apply(real_db, config, first, 0)
    before = work(real_db, tenant)
    other = record(config, dict(v1, application=v1["application"] + "-recreated"))
    with pytest.raises(AppError) as raised:
        apply(real_db, config, other, 0)
    assert raised.value.code == "source_coordinate_conflict"
    # The whole application rolled back, including its admission order and clock.
    assert work(real_db, tenant) == before
    delivery = json.loads(other[2])["event_id"]
    with real_db.transaction(tenant) as conn:
        assert not conn.execute(
            "SELECT 1 FROM kafka_source_results WHERE delivery_id=%s", (delivery,)
        ).fetchone()
        assert not conn.execute(
            "SELECT 1 FROM alerts WHERE tenant_id=%s AND application=%s",
            (tenant, v1["application"] + "-recreated"),
        ).fetchone()
        held = conn.execute(
            "SELECT delivery_id FROM kafka_source_results WHERE topic=%s AND partition=0 AND input_offset=0",
            (first[0],),
        ).fetchone()
    assert str(held["delivery_id"]) == json.loads(first[2])["event_id"]
    # The recorded delivery still replays at its coordinate; the other applies elsewhere.
    assert apply(real_db, config, first, 0) == original
    assert json.loads(apply(real_db, config, other, 1))["tenant_id"] == str(tenant)
