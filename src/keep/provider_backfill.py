"""One-time REV-55 provider backfill for alerts stored before the provider existed.

Run after migration 0060, once every source processor runs a release that stores
providers. Writers keep running: each batch takes the tenant coordinator like
any alert write. Rerunning it changes nothing.
"""

import argparse
import json
from uuid import UUID

from keep.alert_notifications import configure
from keep.alerts.provider_backfill import BATCH, backfill_tenant
from keep.config import Settings
from keep.db import Database


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", type=UUID, help="Backfill one tenant instead of all")
    parser.add_argument("--batch", type=int, default=BATCH, help="Alerts per transaction")
    args = parser.parse_args()
    if not 1 <= args.batch <= 1000:
        parser.error("--batch must be 1 through 1000")
    settings = Settings()
    db = Database(
        settings.database_url.get_secret_value(), 1, deployment_epoch=settings.deployment_epoch
    )
    try:
        # Search hints are best effort; reconciliation repairs a missed one.
        configure(settings)
        if args.tenant:
            tenants = [args.tenant]
        else:
            with db.transaction() as conn:
                tenants = [row["id"] for row in conn.execute("SELECT id FROM tenants ORDER BY id")]
        for tenant in tenants:
            print(json.dumps({"provider_backfill": backfill_tenant(db, tenant, args.batch)}))
    finally:
        db.close()


if __name__ == "__main__":
    main()
