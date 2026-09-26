BEGIN;
ALTER TABLE public.agent_commands ADD COLUMN IF NOT EXISTS request_key text;
CREATE UNIQUE INDEX IF NOT EXISTS agent_commands_request_key_unique ON public.agent_commands(request_key) WHERE request_key IS NOT NULL;
UPDATE public.agent_commands SET status='error',exit_code=coalesce(exit_code,125),result=coalesce(result,'') || E'\n[3.0 migration: uncertain outcome remains an error; no automatic rerun]',finished_at=coalesce(finished_at,clock_timestamp()) WHERE status='blocked';
ALTER TABLE public.agent_commands DROP CONSTRAINT agent_commands_status_check;
ALTER TABLE public.agent_commands ADD CONSTRAINT agent_commands_status_check CHECK(status IN ('pending','running','done','error'));
ALTER TABLE public.agent_commands DROP CONSTRAINT agent_commands_kind_check;
ALTER TABLE public.agent_commands ADD CONSTRAINT agent_commands_kind_check CHECK(kind IN ('shell','file.read','file.read_lines','file.write','file.edit','file.patch','file.delete','file.list','file.stat','file.hash','file.mkdir','code.search','code.glob','git.status','git.diff','workspace.list','workspace.create','job.status','job.output','job.cancel','bridge.health'));
CREATE OR REPLACE FUNCTION public.submit_command(p_command text,p_run_timeout_sec integer DEFAULT NULL,p_kind text DEFAULT 'shell',p_payload jsonb DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $fn$
DECLARE v_id uuid; v_key text; v_payload jsonb:=coalesce(p_payload,'{}'::jsonb); v_command text; existing public.agent_commands%ROWTYPE;
BEGIN
 IF p_kind IS NULL OR p_kind NOT IN ('shell','file.read','file.read_lines','file.write','file.edit','file.patch','file.delete','file.list','file.stat','file.hash','file.mkdir','code.search','code.glob','git.status','git.diff','workspace.list','workspace.create','job.status','job.output','job.cancel','bridge.health') THEN RAISE EXCEPTION 'unsupported kind'; END IF;
 IF jsonb_typeof(v_payload)<>'object' THEN RAISE EXCEPTION 'payload must be an object'; END IF;
 IF p_run_timeout_sec IS NOT NULL AND (p_run_timeout_sec<1 OR p_run_timeout_sec>7200) THEN RAISE EXCEPTION 'runtime must be 1-7200 seconds'; END IF;
 IF p_kind='shell' AND (p_command IS NULL OR btrim(p_command)='') THEN RAISE EXCEPTION 'shell command required'; END IF;
 v_command:=CASE WHEN p_kind='shell' THEN p_command ELSE p_kind END;
 v_key:=nullif(v_payload->>'request_key','');
 IF v_key IS NOT NULL AND length(v_key)>200 THEN RAISE EXCEPTION 'request_key exceeds 200 characters'; END IF;
 IF v_key IS NOT NULL THEN
  PERFORM pg_advisory_xact_lock(hashtextextended(v_key,0));
  SELECT * INTO existing FROM public.agent_commands c WHERE c.request_key=v_key;
  IF FOUND THEN
   IF existing.command IS DISTINCT FROM v_command OR existing.kind IS DISTINCT FROM p_kind OR existing.timeout_sec IS DISTINCT FROM p_run_timeout_sec OR coalesce(existing.payload,'{}') IS DISTINCT FROM v_payload THEN RAISE EXCEPTION 'request_key reused with different request'; END IF;
   RETURN existing.id;
  END IF;
 END IF;
 INSERT INTO public.agent_commands(command,timeout_sec,kind,payload,request_key) VALUES(v_command,p_run_timeout_sec,p_kind,v_payload,v_key) RETURNING id INTO v_id;
 RETURN v_id;
END $fn$;
CREATE OR REPLACE FUNCTION public.command_result(p_id uuid,p_wait_seconds integer DEFAULT 25)
RETURNS TABLE(tracking_id uuid,status text,exit_code integer,result text,duration_ms integer)
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $fn$
DECLARE deadline timestamptz:=clock_timestamp()+make_interval(secs=>least(25,greatest(0,coalesce(p_wait_seconds,25)))); r record;
BEGIN
 LOOP
  SELECT c.status,c.exit_code,c.result,c.duration_ms INTO r FROM public.agent_commands c WHERE c.id=p_id;
  IF NOT FOUND THEN RETURN QUERY SELECT p_id,NULL::text,NULL::integer,NULL::text,NULL::integer; RETURN; END IF;
  EXIT WHEN r.status IN ('done','error') OR clock_timestamp()>=deadline;
  PERFORM pg_sleep(least(0.5,greatest(0,extract(epoch FROM deadline-clock_timestamp()))));
 END LOOP;
 RETURN QUERY SELECT p_id,r.status,r.exit_code,r.result,r.duration_ms;
END $fn$;
-- Compatibility RPCs no longer requeue or block. Coordinator owns adoption.
CREATE OR REPLACE FUNCTION public.requeue_stale_bridge(p_runner text,p_default_timeout integer)
RETURNS SETOF uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $fn$
BEGIN
 IF public.bridge_secret_ok(current_setting('request.headers',true)::json->>'x-bridge-secret') IS NOT TRUE THEN RAISE EXCEPTION 'bridge secret required'; END IF;
 RETURN;
END $fn$;
CREATE OR REPLACE FUNCTION public.reap_orphaned_commands(p_default_timeout integer)
RETURNS SETOF uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $fn$
BEGIN
 IF public.bridge_secret_ok(current_setting('request.headers',true)::json->>'x-bridge-secret') IS NOT TRUE THEN RAISE EXCEPTION 'bridge secret required'; END IF;
 RETURN;
END $fn$;
NOTIFY pgrst,'reload schema';
COMMIT;
