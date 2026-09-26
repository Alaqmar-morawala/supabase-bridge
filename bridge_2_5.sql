BEGIN;
-- Remove the obsolete overload. One four-argument function accepts 1-4 args.
DROP FUNCTION public.submit_command(text, integer);
CREATE OR REPLACE FUNCTION public.submit_command(p_command text, p_run_timeout_sec integer DEFAULT NULL, p_kind text DEFAULT 'shell', p_payload jsonb DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog, public AS $fn$
DECLARE v_id uuid;
BEGIN
  IF p_kind IS NULL OR p_kind NOT IN ('shell','file.read','file.write','file.delete','file.list','file.stat','file.mkdir') THEN RAISE EXCEPTION 'unsupported command kind'; END IF;
  IF p_run_timeout_sec IS NOT NULL AND (p_run_timeout_sec < 1 OR p_run_timeout_sec > 7200) THEN RAISE EXCEPTION 'runtime must be between 1 and 7200 seconds'; END IF;
  IF p_kind = 'shell' AND (p_command IS NULL OR btrim(p_command) = '') THEN RAISE EXCEPTION 'shell command must not be empty'; END IF;
  IF p_kind <> 'shell' AND (p_payload IS NULL OR jsonb_typeof(p_payload) <> 'object') THEN RAISE EXCEPTION 'file payload must be an object'; END IF;
  INSERT INTO public.agent_commands(command,timeout_sec,kind,payload) VALUES (CASE WHEN p_kind='shell' THEN p_command ELSE p_kind END,p_run_timeout_sec,p_kind,p_payload) RETURNING id INTO v_id;
  RETURN v_id;
END $fn$;
CREATE OR REPLACE FUNCTION public.command_result(p_id uuid,p_wait_seconds integer DEFAULT 25)
RETURNS TABLE(tracking_id uuid,status text,exit_code integer,result text,duration_ms integer)
LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog, public AS $fn$
DECLARE deadline timestamptz := clock_timestamp()+make_interval(secs=>least(25,greatest(0,coalesce(p_wait_seconds,25)))); r record;
BEGIN
  LOOP
    SELECT c.status,c.exit_code,c.result,c.duration_ms INTO r FROM public.agent_commands c WHERE c.id=p_id;
    IF NOT FOUND THEN RETURN QUERY SELECT p_id,NULL::text,NULL::integer,NULL::text,NULL::integer; RETURN; END IF;
    EXIT WHEN r.status IN ('done','error','blocked') OR clock_timestamp()>=deadline;
    PERFORM pg_sleep(least(0.5,greatest(0,extract(epoch FROM deadline-clock_timestamp()))));
  END LOOP;
  RETURN QUERY SELECT p_id,r.status,r.exit_code,r.result,r.duration_ms;
END $fn$;
-- Compatibility names retained. Stale claims are blocked, NEVER requeued.
CREATE OR REPLACE FUNCTION public.requeue_stale_bridge(p_runner text,p_default_timeout integer)
RETURNS SETOF uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $fn$
BEGIN
  IF public.bridge_secret_ok(current_setting('request.headers',true)::json->>'x-bridge-secret') IS NOT TRUE THEN RAISE EXCEPTION 'bridge secret required'; END IF;
  RETURN QUERY UPDATE public.agent_commands c SET status='blocked',exit_code=125,result='[manual review required; not re-executing] Stale claim with unknown outcome. Inspect effects and any local receipt.',finished_at=clock_timestamp()
  WHERE c.status='running' AND c.claimed_by=p_runner AND c.executed IS NOT TRUE
    AND c.started_at < clock_timestamp()-greatest(interval '660 seconds',coalesce(c.timeout_sec,p_default_timeout,600)*interval '2 seconds'+interval '60 seconds') RETURNING c.id;
END $fn$;
CREATE OR REPLACE FUNCTION public.reap_orphaned_commands(p_default_timeout integer)
RETURNS SETOF uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $fn$
BEGIN
  IF public.bridge_secret_ok(current_setting('request.headers',true)::json->>'x-bridge-secret') IS NOT TRUE THEN RAISE EXCEPTION 'bridge secret required'; END IF;
  RETURN QUERY UPDATE public.agent_commands c SET status='blocked',exit_code=125,result='[manual review required; not re-executing] Runner did not deliver a result. Preserve receipts and reconcile external effects.',finished_at=clock_timestamp()
  WHERE c.status='running' AND c.started_at < clock_timestamp()-greatest(interval '24 hours',coalesce(c.timeout_sec,p_default_timeout,600)*interval '3 seconds'+interval '300 seconds') RETURNING c.id;
END $fn$;
NOTIFY pgrst,'reload schema';
COMMIT;
