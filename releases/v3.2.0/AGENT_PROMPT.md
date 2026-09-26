# Supabase-only local-agent bridge 3.2

## Contract

This is a full-trust local development bridge. Shell commands are not filtered.
Default workspace: `/home/alaqmar/Desktop/Auto Bug Bounty` (a multi-repository
folder, not itself a Git repository). The `bridge` workspace remains available.
No security scans, project scripts, builds or exploits are run by installation.

Queue states are pending, running, done, error. There is NO Blocked state.
An uncertain mutation ends with error 125 and is not blindly repeated. Do not
turn an uncertain outcome into a fresh duplicate submission without checking
its effects. No bridge can guarantee exactly-once external side effects.

## Submit and wait

```sql
select public.submit_command('pwd',30,'shell',
 '{"workspace":"main","request_key":"unique-read-001"}'::jsonb) as tracking_id;
select public.submit_command('make',3600,'shell',
 '{"workspace":"main","cwd":"relative/repository","scope":"workspace","request_key":"build-unique-001"}'::jsonb);
select public.command_result_v2('<id>',20);
-- Existing clients can still use: select * from public.command_result('<id>',25);
```

Runtime 1-7200 seconds, default 600. Prefer the v2 result envelope with a
20-second wait; the wait is also capped by the caller's statement_timeout and
the effective wait is reported in the continuation hint. Poll the same ID while
pending/running. Always include a stable, unique payload.request_key for a
mutation. Identical requests with the same key return the original tracking ID,
even after its result row is cleaned up. A separate protected ledger keeps the
key, request fingerprint, and ID until an administrator deliberately removes it.
Reusing a key with different arguments is an error. Different keys are different
jobs. Requests without keys have no duplicate-submission protection.
The ledger protects queue submission, not arbitrary external side effects.

The coordinator polls every `active_poll_interval_sec` (default 0.25 s) while
jobs are live or work was found, and every `poll_interval_sec` (default 3 s)
when idle. Typical submit-to-done latency for a small read is now under one
second plus systemd spawn time.

### Dispatch expiry

A pending request that waits longer than its allowed queue age is never
executed. It ends as error 124 with error_code `EXPIRED_BEFORE_DISPATCH`,
`started_at` null, `executed` false and `partial_effects_possible` false.
Defaults come from config: `max_queue_age_sec_write` (900 s) for shell and
every mutation, `max_queue_age_sec_safe` (3600 s) for safe reads. A request may
shorten its own limit with payload `max_queue_age_sec` (1-86400); it cannot
extend the configured default. Submit a new request with a new key if the work
is still wanted; do not treat expiry as permission to skip re-confirmation.

Absolute or relative cwd overrides are supported. Workspace names come from
workspace.list. Shell commands retain unrestricted user-account access.

## Result envelope (protocol 3.2)

`command_result_v2(id,20)` returns protocol_version, tracking_id, status, kind,
exit_code, duration_ms, started_at, data, error_code, partial_effects_possible,
continuation, and resubmit:false. `started_at` is null for work that never
started (expired, cancelled before launch). `partial_effects_possible` is true
only for an error on a mutation kind that actually started. When a bridge error
envelope is embedded in the result, its error_code is surfaced, including
`EXPIRED_BEFORE_DISPATCH` and `BATCH_ITEM_FAILED`. Exit 124 = timeout or
expiry; 125 = uncertain lost outcome; 126 = launch/bridge failure; 130 =
cancellation. result_expired and not_found are lookup outcomes, not queue states.

## Job control and incremental output

```sql
select public.submit_command(null,30,'job.status','{"id":"<job-id>"}'::jsonb);
select public.submit_command(null,30,'job.output',
 '{"id":"<job-id>","offset":0,"length":100000,"text":true}'::jsonb);
select public.submit_command(null,30,'job.cancel','{"id":"<job-id>"}'::jsonb);
select public.submit_command(null,30,'bridge.health','{}'::jsonb);
```

Each control request also returns its own tracking ID. Use command_result_v2 on
that ID. job.status includes the lease (phase names the lock being awaited, for
example `waiting_for_lock:ws:main`), the attempt, and the job's lock plan.
job.output returns next_offset, has_more, eof, and size. Persist the cursor. Use
text:false for base64 byte fidelity. UTF-8 text pages may split a character.
File-operation results are in the result envelope, not shell log output.
Controls are serviced even while normal job slots are occupied.

## Files and reliable edits

```sql
select public.submit_command(null,30,'file.read',
 '{"path":"README.md","text":true,"offset":0,"length":600000,"include_hash":true}'::jsonb);
select public.submit_command(null,30,'file.read_lines',
 '{"path":"src/main.py","start_line":1,"count":200}'::jsonb);
select public.submit_command(null,30,'file.hash','{"path":"src/main.py"}'::jsonb);
select public.submit_command(null,30,'file.write',
 '{"path":"new.txt","text":"hello","expected_sha256":"missing"}'::jsonb);
select public.submit_command(null,30,'file.edit',
 '{"path":"src/main.py","expected_sha256":"<current hash>","edits":[{"old":"unique old text","new":"replacement"}]}'::jsonb);
select public.submit_command(null,30,'file.patch',
 '{"path":"src/main.py","expected_sha256":"<current hash>","patch":"<single-file unified diff>"}'::jsonb);
```

file.write creates or atomically replaces; overwriting or appending an existing
file REQUIRES expected_sha256. New files may use missing. Edits require each old
text to occur exactly once. Unified patches require exact context; one file per
operation, no fuzzy matching. No-newline patches should use file.edit instead.
All replacements use temporary-file, fsync, and atomic rename. Modes are kept.
Hash rechecks detect external edits, but noncooperating external writers are not
locked. Review diffs.

file.read/write limits: 4,000,000 bytes per operation. Regular files only for
read/write; pipes/devices rejected. Base64 via content_base64 supports binary
writes. file.read defaults to base64. file.list/stat/mkdir/delete are supported;
recursive deletion requires recursive:true. file.list is paged (offset/limit),
with an explicit 10,000-entry scan cap. Read_lines caps page size at 1 MB.

## Batch reads

```sql
select public.submit_command(null,60,'batch',jsonb_build_object(
 'workspace','main','request_key','batch-unique-001',
 'items',jsonb_build_array(
  jsonb_build_object('kind','file.read','payload',jsonb_build_object('path','repo/README.md','text',true,'include_hash',true)),
  jsonb_build_object('kind','file.hash','payload',jsonb_build_object('path','repo/src/main.py')),
  jsonb_build_object('kind','git.status','payload',jsonb_build_object('cwd','repo')))));
```

A batch runs 1-50 safe operations sequentially in one job: file.read,
file.read_lines, file.hash, file.stat, file.list, code.search, code.glob,
git.status, git.diff. Item payloads follow the standalone rules and may set a
relative or absolute cwd, but not workspace, request_key, max_queue_age_sec,
scope or items. The result has items[] with index, kind, status and either data
or error/error_code, plus count, requested and failed. An item failure never
aborts the batch; the job exits 1 with error_code BATCH_ITEM_FAILED when any
item failed, and partial_effects_possible stays false. Per-item results are
capped at 4 MB and the whole batch at 32 MB (truncated_at_index reports where
it stopped). Batch takes shared locks only.

## Large-codebase navigation

```sql
select public.submit_command(null,30,'code.search',
 '{"pattern":"function_name","glob":"*.py","limit":100}'::jsonb);
select public.submit_command(null,30,'code.glob',
 '{"pattern":"**/test_*.py","limit":1000}'::jsonb);
select public.submit_command(null,30,'git.status',
 '{"cwd":"relative/repository"}'::jsonb);
select public.submit_command(null,30,'git.diff',
 '{"cwd":"relative/repository","path":"src/main.py"}'::jsonb);
```

Search is literal by default (regex:true explicitly enables regex), honors
repository ignore rules and skips hidden paths unless hidden:true. Results
are capped and report truncation. Narrow glob/pattern when truncated. Commands
use ripgrep and Git directly with bounded output and time. Git status/diff do
not run hooks, textconv or external diff helpers. They require a Git repo cwd.

## Workspaces

```sql
select public.submit_command(null,30,'workspace.list','{}'::jsonb);
select public.submit_command(null,120,'workspace.create',
 '{"cwd":"relative/repository","name":"feature-copy","branch":"feature/bridge-work","ref":"HEAD"}'::jsonb);
```

workspace.create explicitly creates a Git worktree and a new branch under
~/supabase-bridge/workspaces, then registers the name. It can leave partial
changes on failure. It always holds the global exclusive lock.

## Locks, concurrency and recovery

Every job holds the global lock at least shared. Structured reads take a shared
lock on the workspace that contains their resolved path; structured mutations
take that workspace lock exclusively. Shell holds the global lock exclusively
unless the request declares `"scope":"workspace"` and its cwd is inside the
workspace root; the declaration is a contract, not filesystem confinement, so
declare it only for commands that stay inside that workspace. Paths outside
every registered workspace, undeclared shell and workspace.create use the
global exclusive lock and still exclude everything. Two writers in the same
workspace serialize; readers and writers in different workspaces proceed
together up to max_parallel_jobs. The dispatcher skips a queued job whose locks
conflict with live jobs and dispatches later non-conflicting jobs.

Jobs run in independent systemd user units, with immutable release code paths,
private durable requests/results, stdout logs, leases and cancellation records.
Bridge-only restarts adopt surviving jobs. A laptop reboot cannot preserve a
process; safe read-only jobs may retry up to two times, mutations end as errors
when their outcome is unknown. Shell commands are NEVER classified safe just
because their text looks read-only. Known safe operations only are retryable.
Control requests use a separate lane. This is not an interactive PTY or IDE;
there is no stdin conversation, browser automation or persistent terminal API.

Each new job has a 32 GiB memory limit and a limit of 4,096 processes/threads.
Two normal job slots remain enabled. Two concurrent jobs can use up to 64 GiB
combined; these are ceilings, not reserved memory.

Settings in `~/supabase-bridge/config.json`:
- `job_memory_max`: `32G`
- `job_tasks_max`: `4096`
- `max_parallel_jobs`: `2`
- `poll_interval_sec`: `3` (idle), `active_poll_interval_sec`: `0.25` (default when absent)
- `max_queue_age_sec_write`: `900`, `max_queue_age_sec_safe`: `3600` (defaults when absent)

The bridge reloads configuration each polling cycle. New jobs receive the new
limits without a restart; running jobs retain their original limits. The
coordinator has separate service limits. Each job has a deadline independent
of the coordinator. Disk dispatch pauses below 256 MB free. Output is capped at
1 GB/job with explicit loss reporting. Retention removes positively confirmed
delivered results after seven days. Unknown artifacts are kept. Full-trust
shell commands can still change system state.

## Health

```sql
select last_seen,version,hostname,last_error,last_error_at,stats from public.bridge_status;
```

The coordinator heartbeats at most once per second while active and once per
idle pass. `stats` carries: active_jobs, queue_depth (exact pending count),
oldest_pending_age_sec, current_exclusive_job (id, kind, started_at,
deadline_at, locks) or null, last_finished_at, errors_by_code, done, error,
free_bytes, release (path, version and SHA-256 of agent_bridge.py,
workspace_ops.py, bridge_lib.py), schema_mismatch (object names whose deployed
definition differs from expected_schema.json; empty when clean), schema_check
and schema_checked_at. bridge.health returns the same stats plus configuration.
last_error reports dispatch or connectivity problems; individual job errors live
on their rows. A healthy heartbeat does not mean every job succeeded. Check
results and job.status.

## Maintenance

Current launcher: ~/supabase-bridge/supabase_bridge.py
Immutable release: ~/supabase-bridge/releases/v3.2.0
Previous release (rollback target): ~/supabase-bridge/releases/v3.1.0
Data: ~/supabase-bridge/jobs, workspaces.json, locks, state.json
Schema files, in order: bridge_3_0.sql, bridge_3_1.sql, bridge_3_1_1.sql, bridge_3_2.sql
Expected schema fingerprints: releases/v3.2.0/expected_schema.json
Cutover and rollback: releases/v3.2.0/cutover.sh (run from an independent systemd unit)

Use `systemctl --user restart supabase-bridge.service` for coordinator restart.
Job units continue independently. To cancel a job use job.cancel. Do not kill
or reset rows to retry. The protocol has no promises of zero failures or
unlimited resources; it reports failures instead of silently claiming success.

## Hardening contract (3.2)

This path uses Supabase only. No tunnels, public endpoints, or typed MCP server
are required. The separate typed-mcp prototype remains disabled and unused.

Unknown payload fields and wrong argument types are rejected before queuing.
Use real JSON booleans, not strings. Request contents and execution markers
cannot be rewritten to turn an old command into new work. Do not reset terminal
rows or force running rows back to pending. Shell contents remain unrestricted.

Lost completion replies are reconciled against database state. Cleanup first
confirms a terminal row and its retention age, records local cleanup intent,
and confirms remote deletion before deleting local evidence. The request-key
ledger is not deleted by cleanup. Unknown/missing rows retain local evidence
unless a previous verified cleanup intent exists. Disk pressure may therefore
need an explicit operator review; nothing is silently deleted to make space.

An unknown systemd state is NOT proof that a job is dead. It occupies its slot
and cannot trigger a duplicate retry. Cancellation is requested, not claimed
complete, until the worker exit/result is known. Independent jobs continue
where capacity and lock compatibility permit. bridge.health and job.status
include protocol/resource/phase hints. Cleanup errors are reported separately.

File content and its hash come from one open-file snapshot. If inode, size,
mtime or ctime changes during the read, the read fails with FILE_CHANGED rather
than attaching a new hash to stale data. Hash-checked edits also verify the
content used to compute the change. External writers that ignore bridge locks
can still race the final filesystem rename; review diffs and use worktrees.

Schema drift is detected, not repaired: the coordinator compares live
fingerprints with expected_schema.json at startup and hourly and reports
differences in stats. Re-export expected_schema.json only as part of a
versioned release.

## Release acceptance and limits

Run the installed suite from releases/v3.2.0:
`python3 -W error::ResourceWarning -m unittest -v test_v3 test_hardening test_v32`
The suite uses disposable directories and short-lived systemd test jobs.
Run database transaction tests only against the documented test procedure.
Do not simulate failure by rebooting, disconnecting networking or filling the
real laptop disk without approval.

The release tests acknowledgements lost before/after completion, repeated
transport failure, unknown worker status, cancellation timing, cleanup failure,
strict validation, stale reads, atomic-write disk failure, lock planning and
conflicts, dispatch expiry, batch validation and execution, stats assembly and
the library split. These are bounded fault simulations, not real power-loss or
multi-day outage certification. System package updates, storage failure,
account compromise, manual database changes, and duplicate actions with new
request keys remain operational risks.
