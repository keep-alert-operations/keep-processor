# appchi-processor

Kafka source processing, immutable PostgreSQL replay proof and transactional changed-event publication.

See [system design](https://github.com/appchi-alert-operations/appchi-design), [workspace setup](https://github.com/appchi-alert-operations/appchi-delivery) and [source provenance](SOURCE.md).

## Development

Use Python 3.13 and the committed `uv.lock`. Configure Git authentication for the
private organization repositories before resolving internal dependencies.

```powershell
uv sync --locked --all-extras
uv run --locked ruff check src tests
uv run --locked pytest
uv build --wheel
```

Shared `appchi` modules use namespace packaging; each module has exactly one owning
distribution. Dependencies resolve immutable Git revisions in Python dependency metadata and `uv.lock`.
For coordinated local edits use the delivery workspace setup, which installs local
packages without changing release dependency pins.

## Execution

`uv run --locked appchi-processor` is the entrypoint. Supply the documented environment
bindings; package installation does not start services or migrate data.
