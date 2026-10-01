import os
from uuid import uuid4
import pytest



@pytest.fixture(scope="session")
def real_db():
    dsn = os.environ.get("APPCHI_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set APPCHI_TEST_DATABASE_URL to an isolated real PostgreSQL database")
    from appchi.migrate import migrate, migration_directory
    from appchi.db import Database
    migrate(dsn, migration_directory())
    database = Database(dsn)
    yield database
    database.close()


@pytest.fixture
def tenant(real_db):
    tenant = uuid4()
    with real_db.transaction() as conn:
        conn.execute(
            "INSERT INTO tenants(id,kind,name,name_key) VALUES(%s,'specific',%s,%s)",
            (tenant, str(tenant), str(tenant)),
        )
        conn.execute("INSERT INTO tenant_work_state(tenant_id) VALUES(%s)", (tenant,))
        conn.execute("INSERT INTO tenant_view_state(tenant_id) VALUES(%s)", (tenant,))
        conn.execute("INSERT INTO tenant_configuration_guard(tenant_id) VALUES(%s)", (tenant,))
        conn.execute("INSERT INTO admission_capacity(scope) VALUES(%s)", (str(tenant),))
    return tenant


@pytest.fixture
def cipher():
    from appchi.shared.crypto import EnvelopeCipher
    return EnvelopeCipher({"test": b"k" * 32}, "test")


@pytest.fixture
def profile():
    return {
        "id": "test",
        "global_items": 2250000,
        "tenant_items": 1800000,
        "global_bytes": 10**9,
        "tenant_bytes": 10**9,
        "byte_multiplier": 4,
        "item_overhead": 2048,
    }


@pytest.fixture
def v1():
    return {
        "application": "test-" + str(uuid4()),
        "object": "cpu",
        "node_name": "n1",
        "message": "firing",
        "operator": "legacy",
        "time_created": "01-01-2026 00:00:00",
        "severity": "error",
    }
