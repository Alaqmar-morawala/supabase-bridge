# Local-agent bridge 3.0

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
select public.submit_command('pwd') as tracking_id;
select public.submit_command('make',3600,'shell',
 '{"workspace":"main","cwd":"relative/repository","request_key":"build-unique-001"}'::jsonb);
select * from public.command_result('<id>',25);
```

Runtime 1-7200 seconds, default 600. Wait 0-25 seconds. Poll the same ID while
pending/running. Supplying payload.request_key deduplicates IDENTICAL requests
while the row exists (normally at least seven days). Reusing a key with different
arguments is an error. Different keys are different jobs. Do not rely on a key
as permanent application-level idempotency.

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

Each job has a 2 GB memory limit and 512 tasks; change configuration deliberately
for larger builds. The service's limits still apply to the coordinator. A job
has a deadline independent of the coordinator. Disk dispatch pauses below
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
Immutable release: ~/supabase-bridge/releases/v3.0.0
Data: ~/supabase-bridge/jobs, workspaces.json, locks
Backup and rollback: ~/supabase-bridge/upgrade-v3-backup-ygbrso8t

Use `systemctl --user restart supabase-bridge.service` for coordinator restart.
Job units continue independently. To cancel a job use job.cancel. Do not kill
or reset rows to retry. The protocol has no promises of zero failures or
unlimited resources; it reports failures instead of silently claiming success.
