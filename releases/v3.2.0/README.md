# Supabase bridge 3.2 release

See AGENT_PROMPT.md for the Supabase-only operating protocol. No additional
external connection is needed. The typed-mcp prototype is not used.

## What changed since 3.1
- Adaptive dispatch polling: the coordinator polls every `active_poll_interval_sec`
  (default 0.25 s) while jobs are live or work was found, and every
  `poll_interval_sec` when idle. Error backoff is unchanged.
- Workspace-scoped locks: structured reads and mutations lock only the workspace
  that contains their path. Shell still holds the global exclusive lock unless the
  request declares `scope: "workspace"`. A global writer still excludes everything.
- Dispatch expiry: pending requests older than `max_queue_age_sec_write` (default
  900 s) for shell and mutations, or `max_queue_age_sec_safe` (default 3600 s) for
  safe reads, end as error 124 `EXPIRED_BEFORE_DISPATCH` without executing. A
  request may shorten its own limit with payload `max_queue_age_sec`.
- `batch` kind: up to 50 safe read operations in one job with per-item results.
- Richer `bridge_status.stats`: queue depth, oldest pending age, current exclusive
  job, last completion, error counts by code, free disk, release hashes, and the
  schema fingerprint check result.
- Schema fingerprint: `bridge_schema_fingerprint()` plus `expected_schema.json`;
  the coordinator reports drift in `stats.schema_mismatch` at startup and hourly.
- `bridge_core.py` is replaced by the library-only `bridge_lib.py`. The legacy 2.5
  daemon and its `blocked` status vocabulary are gone.
- Result envelope protocol 3.2 adds `started_at`; `partial_effects_possible` is
  false for work that never started; embedded bridge error codes are surfaced.

## Release contents
- agent_bridge.py: coordinator, worker, recovery, cancellation, cleanup, stats.
- workspace_ops.py: strict payload checks, coherent reads, hash-checked edits, batch.
- bridge_lib.py: transport, capture and atomic-write helpers (library only).
- bridge_3_0.sql, bridge_3_1.sql, bridge_3_1_1.sql, bridge_3_2.sql: ordered schema.
- expected_schema.json: fingerprints of the deployed schema objects.
- test_v3.py, test_hardening.py, test_v32.py: regression, isolated failure and 3.2 tests.
- cutover.sh: supervised coordinator cutover with automatic rollback.
- launcher.py: copy of the root launcher pointing at this release.

Configuration keys are read from ~/supabase-bridge/config.json. New keys default
when absent: active_poll_interval_sec 0.25, max_queue_age_sec_safe 3600,
max_queue_age_sec_write 900.

## Deployment
Run the suite from this directory:
`python3 -W error::ResourceWarning -m unittest -v test_v3 test_hardening test_v32`
Apply SQL in order, validating each file first inside a rolled-back transaction.
Cut over with cutover.sh from an independent systemd unit, never from inside a
bridge shell job. It backs up the launcher, config and state, switches the launcher,
restarts the coordinator, waits for a fresh state.json with the new version, and
reverts to the previous release automatically if health is not reached.
Rollback keeps the additive SQL; 3.1 tolerates the 3.2 functions.
