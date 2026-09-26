# Supabase bridge 3.1 hardening release

See AGENT_PROMPT.md for the Supabase-only operating protocol. No additional
external connection is needed. The typed-mcp prototype is not used.

## Release contents
- agent_bridge.py: coordinator, worker, recovery, cancellation, cleanup.
- workspace_ops.py: strict payload checks and coherent content/hash reads.
- bridge_core.py: capture and authenticated transport helpers (8-second IO cap).
- bridge_3_1.sql: durable RLS-protected request-key ledger, immutable request
  guards, payload validation, indexes, and command_result_v2 JSON envelope.
- test_v3.py and test_hardening.py: regression and isolated failure tests.
- hardening-test-results.txt: recorded local acceptance output.

Current resource limits are loaded from the root config, not release defaults:
32 GiB/job, 4096 tasks/job, two read-only job slots. Shell and writes serialize.

Migration is additive except stricter validation and request immutability.
Legacy submit_command and command_result signatures remain available.
The independent request ledger is deliberately not expired with result cleanup.
Existing keys are backfilled; commands historically submitted without keys
cannot be deduplicated retrospectively.

Deployment requires an idle queue, backup, checksum checks, a transactionally
verified SQL migration, independent systemd deploy unit, a new versioned release,
and post-start heartbeat/sentinel checks. Rollback restores previous local
files while retaining durable deduplication and the additive SQL functions.
A full SQL rollback is documented but would abandon new guarantees and requires
explicit operator review. Never silently delete the request ledger.
