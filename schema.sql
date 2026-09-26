-- supabase-bridge schema — run once in the Supabase SQL Editor
-- (or have the cloud agent run it via the Supabase MCP execute_sql tool).

-- Command queue: the cloud agent inserts a row, the local bridge daemon on the
-- laptop claims it, executes it, and writes the outcome back into the same row.
create table if not exists public.agent_commands (
  id          uuid primary key default gen_random_uuid(),
  command     text not null,
  status      text not null default 'pending'
              check (status in ('pending', 'running', 'done', 'error', 'blocked')),
  result      text,
  exit_code   int,
  created_at  timestamptz not null default now(),
  started_at  timestamptz,
  finished_at timestamptz
);

create index if not exists agent_commands_pending_idx
  on public.agent_commands (created_at)
  where status = 'pending';

-- Heartbeat: the bridge updates this row on every poll pass so the cloud agent
-- can tell whether the laptop is reachable before queueing work.
create table if not exists public.bridge_status (
  id        int primary key default 1,
  last_seen timestamptz,
  version   text
);
insert into public.bridge_status (id) values (1) on conflict do nothing;

-- Lock down: no policies are created, so anon/authenticated clients see
-- nothing. The service key used by the local daemon bypasses RLS — that key
-- lives only in ~/supabase-bridge/.env (chmod 600) on the laptop.
alter table public.agent_commands enable row level security;
alter table public.bridge_status  enable row level security;

-- Optional housekeeping (run occasionally, or let the cloud agent do it):
--   delete from public.agent_commands
--   where status in ('done','error','blocked') and finished_at < now() - interval '7 days';
