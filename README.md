# keep-processor

Kafka source processing, immutable PostgreSQL replay proof and transactional changed-event publication.

See [system design](https://github.com/keep-alert-operations/keep-design), [workspace setup](https://github.com/keep-alert-operations/keep-delivery) and [source provenance](SOURCE.md).

## Development

Use Python 3.13, Poetry 2.3.1 and the committed `poetry.lock`. Configure Git authentication for the
private organization repositories before resolving internal dependencies.

```powershell
poetry check --lock
poetry sync --all-extras
poetry run ruff check src tests
poetry run pytest
poetry build --format wheel
```

Shared `keep` modules use namespace packaging; each module has exactly one owning
distribution. Dependencies resolve immutable Git revisions in Python dependency metadata and `poetry.lock`.
For coordinated local edits use the delivery workspace setup, which installs local
packages without changing release dependency pins.

## Execution

`poetry run keep-processor` is the entrypoint. Supply the documented environment
bindings; package installation does not start services or migrate data.

`python -m keep.provider_backfill` is the one-time REV-55 provider backfill. Run it
with the processor's bindings after migration 0060, once every source processor
stores providers. It sets `provider` and `alert_rule_url` on each live alert that
lacks a provider, from the alert's latest applied source event, and publishes a
non-semantic projection change. Writers can keep running, and a rerun changes
nothing. It prints one JSON line per tenant; `unresolved` lists alerts whose latest
source event is missing. `--tenant` limits it to one tenant.
