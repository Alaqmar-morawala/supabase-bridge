# Supabase-only local-agent bridge 3.1

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
 '{"workspace":"main","cwd":"relative/repository","request_key":"build-unique-001"}'::jsonb);
select public.command_result_v2('<id>',20);
-- Existing clients can still use: select * from public.command_result('<id>',25);
```

Runtime 1-7200 seconds, default 600. Prefer the v2 result envelope with a 20-second wait; the legacy result call still supports up to 25 seconds. Poll the same ID while
pending/running. Always include a stable, unique payload.request_key for a
mutation. Identical requests with the same key return the original tracking ID,
even after its result row is cleaned up. A separate protected ledger keeps the
key, request fingerprint, and ID until an administrator deliberately removes it.
Reusing a key with different arguments is an error. Different keys are different
jobs. Existing requests without keys have no duplicate-submission protection.
The ledger protects queue submission, not arbitrary external side effects.

Absolute or relative cwd overrides are supported. Workspace names come from
workspace.list. Shell commands retain unrestricted user-account access.

## Job control and incremental output

```sql
select public.submit_command(null,30,'job.status','{"id":"<job-id>"}'::jsonb);
select public.submit_command(null,30,'job.output',
 '{"id":"<job-id>","offset":0,"length":100000,"text":true}'::jsonb);
select public.submit_command(null,30,'job.cancel','{"id":"<job-id>"}'::jsonb);
select public.submit_command(null,30,'bridge.health','{}'::jsonb);
```

Each control request also returns its own tracking ID. Use command_result on
that ID. job.output returns next_offset, has_more, eof, and size. Persist the
cursor. Use text:false for base64 byte fidelity. UTF-8 text pages may split a
character. File-operation results are in command_result, not shell log output.
Controls are serviced even while normal job slots are occupied.

Exit 124 = timeout; 125 = uncertain lost outcome; 126 = launch/bridge failure;
130 = cancellation. Errors and cancellation may leave partial external effects.

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
locked. Review diffs. Bridge jobs use a global shared/exclusive file lock.

file.read/write limits: 4,000,000 bytes per operation. Regular files only for
read/write; pipes/devices rejected. Base64 via content_base64 supports binary
writes. file.read defaults to base64. file.list/stat/mkdir/delete are supported;
recursive deletion requires recursive:true. file.list is paged (offset/limit),
with an explicit 10,000-entry scan cap. Read_lines caps page size at 1 MB.

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
changes on failure. No existing repository is modified during installation.

## Jobs, concurrency and recovery

Jobs run in independent systemd user units, with immutable release code paths,
private durable requests/results, stdout logs, leases and cancellation records.
Bridge-only restarts adopt surviving jobs. A laptop reboot cannot preserve a
process; safe read-only jobs may retry up to two times, mutations end as errors
when their outcome is unknown. Shell commands are NEVER classified safe just
because their text looks read-only. Known safe operations only are retryable.

Two normal job slots are enabled. Known read-only jobs may run together. Shell
and mutations are serialized with a global exclusive lock because unrestricted
commands and absolute paths can cross workspace boundaries. Independent
worktrees are supported but do not remove this conservative write lock.
Control requests use a separate lane. This is not an interactive PTY or IDE;
there is no stdin conversation, browser automation or persistent terminal API.

Each new job has a 32 GiB memory limit and a limit of 4,096 processes/threads.
Two normal job slots remain enabled. Two concurrent jobs can use up to 64 GiB
combined; these are ceilings, not reserved memory.

Settings in `~/supabase-bridge/config.json`:
- `job_memory_max`: `32G`
- `job_tasks_max`: `4096`
- `max_parallel_jobs`: `2`

The bridge reloads configuration each polling cycle. New jobs receive the new
limits without a restart; running jobs retain their original limits. The
coordinator has separate service limits. Each job has a deadline independent
of the coordinator. Disk dispatch pauses below
256 MB free. Output is capped at 1 GB/job with explicit loss reporting. Retention
removes positively confirmed delivered results after seven days. Unknown
artifacts are kept. Full-trust shell commands can still change system state.

## Health

```sql
select last_seen,version,hostname,last_error,last_error_at,stats from public.bridge_status;
```

The coordinator heartbeats independently of jobs. last_error reports dispatch
or connectivity problems; individual job errors live on their rows. A healthy
heartbeat does not mean every job succeeded. Check results and job.status.

## Maintenance

Current launcher: ~/supabase-bridge/supabase_bridge.py
Immutable release: ~/supabase-bridge/releases/v3.1.0
Data: ~/supabase-bridge/jobs, workspaces.json, locks
Backup and rollback: ~/supabase-bridge/hardening-backup-2gj5q3in

Use `systemctl --user restart supabase-bridge.service` for coordinator restart.
Job units continue independently. To cancel a job use job.cancel. Do not kill
or reset rows to retry. The protocol has no promises of zero failures or
unlimited resources; it reports failures instead of silently claiming success.


## Hardening contract (3.1)

This path uses Supabase only. No tunnels, public endpoints, or typed MCP server
are required. The separate typed-mcp prototype remains disabled and unused.

The v2 wait is capped to the caller's database statement timeout with a safety
margin. Direct REST callers may receive pending/running sooner than requested.
Follow the returned wait hint and keep the same tracking ID. Do not disable
database timeouts to force a longer wait.

`command_result_v2(id,20)` returns a JSON envelope with protocol_version,
tracking_id, status, kind, exit_code, duration_ms, data, error_code,
partial_effects_possible, continuation, and resubmit:false. It explains expired
results separately from unknown/not-visible IDs. Never blindly resubmit an
expired result: recover the original effects or start an explicitly new action.
A queue row still has only pending/running/done/error states. result_expired and
not_found are lookup outcomes, not extra queue lifecycle states.

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
where capacity and write-serialization permit. bridge.health and job.status
include protocol/resource/phase hints. Cleanup errors are reported separately.

File content and its hash come from one open-file snapshot. If inode, size,
mtime or ctime changes during the read, the read fails with FILE_CHANGED rather
than attaching a new hash to stale data. Hash-checked edits also verify the
content used to compute the change. External writers that ignore bridge locks
can still race the final filesystem rename; review diffs and use worktrees.

## Release acceptance and limits

Run the installed suite from releases/v3.1.0:
`python3 -W error::ResourceWarning -m unittest -v test_v3 test_hardening`
The suite uses disposable directories and short-lived systemd test jobs.
Run database transaction tests only against the documented test procedure.
Do not simulate failure by rebooting, disconnecting networking or filling the
real laptop disk without approval.

The release tests acknowledgements lost before/after completion, repeated
transport failure, unknown worker status, cancellation timing, cleanup failure,
strict validation, stale reads, and atomic-write disk failure. These are bounded
fault simulations, not real power-loss or multi-day outage certification.
System package updates, storage failure, account compromise, manual database
changes, and duplicate actions with new request keys remain operational risks.
