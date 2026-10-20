# appchi-processor

Kafka source processing, immutable PostgreSQL replay proof and transactional changed-event publication.

Read the workspace-level `../AGENTS.md` and instructions scoped to affected files.
Choose task-relevant sources from the [context map](../appchi-design/context/map.md)
using intent, paths and behavioral impact. For a local edit, start with local code,
README guidance and affected checks. For behavior, architecture or cross-repository
changes, include relevant system design, current worker scope and affected contracts.
Read source introductions/scope corrections with selected sections; expand when new
dependencies or uncertainty appear. All applicable requirements remain binding.
If working from a standalone clone, use the canonical [workspace guidance](https://github.com/appchi-alert-operations/appchi-delivery/blob/main/workspace/AGENTS.md)
and [design](https://github.com/appchi-alert-operations/appchi-design/tree/main/specs).

This repository owns its source, tests and artifact. Runtime imports must not
depend on another service repository. Shared packages may depend only on their
declared lower-level packages. Cross-repository changes require compatible pinned
dependencies, affected consumer verification and updated delivery evidence.

Use the README and committed manifests for commands. Preserve unrelated changes,
migration checksums, protocol field names, tenant/recovery safeguards and deferred
worker scope. The extracted source includes historical checks; their evidence is
not proof for this revision. Production qualification remains separate.
