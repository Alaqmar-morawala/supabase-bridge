BEGIN;
CREATE TABLE public.agent_request_ledger (
 request_key text PRIMARY KEY CHECK(length(request_key) BETWEEN 1 AND 200),
 request_hash bytea NOT NULL CHECK(octet_length(request_hash)=32),
 command_id uuid NOT NULL UNIQUE,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE public.agent_request_ledger ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.agent_request_ledger FROM PUBLIC,anon,authenticated,service_role;
GRANT SELECT,INSERT ON public.agent_request_ledger TO anon,service_role;
GRANT SELECT ON public.agent_request_ledger TO authenticated;
CREATE POLICY ledger_read ON public.agent_request_ledger FOR SELECT TO anon USING (public.bridge_secret_ok(current_setting('request.headers',true)::json->>'x-bridge-secret') IS TRUE);
CREATE POLICY ledger_insert ON public.agent_request_ledger FOR INSERT TO anon WITH CHECK (public.bridge_secret_ok(current_setting('request.headers',true)::json->>'x-bridge-secret') IS TRUE);
CREATE OR REPLACE FUNCTION public.agent_request_hash(p_command text,p_timeout integer,p_kind text,p_payload jsonb)
RETURNS bytea LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog AS $f$
 SELECT sha256(convert_to(jsonb_build_object('command',p_command,'timeout',p_timeout,'kind',p_kind,'payload',coalesce(p_payload,'{}'::jsonb))::text,'UTF8'))
$f$;
INSERT INTO public.agent_request_ledger(request_key,request_hash,command_id,created_at)
SELECT request_key,public.agent_request_hash(command,timeout_sec,kind,payload),id,created_at FROM public.agent_commands WHERE request_key IS NOT NULL;
CREATE OR REPLACE FUNCTION public.validate_agent_request(p_kind text,p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $f$
DECLARE k text;v jsonb;n numeric;allowed text[];common text[]:=ARRAY['workspace','cwd','request_key'];i jsonb;
BEGIN
 IF p_kind IS NULL OR p_kind NOT IN ('shell','file.read','file.read_lines','file.write','file.edit','file.patch','file.delete','file.list','file.stat','file.hash','file.mkdir','code.search','code.glob','git.status','git.diff','workspace.list','workspace.create','job.status','job.output','job.cancel','bridge.health') THEN RAISE EXCEPTION 'INVALID_ARGUMENT: unsupported kind'; END IF;
 IF p IS NULL OR jsonb_typeof(p)<>'object' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: payload must be an object'; END IF;
 IF octet_length(p::text)>24000000 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: payload exceeds 24 MB'; END IF;
 allowed:=common || CASE p_kind
 WHEN 'shell' THEN ARRAY[]::text[]
 WHEN 'file.read' THEN ARRAY['path','offset','length','text','include_hash']
 WHEN 'file.read_lines' THEN ARRAY['path','start_line','count']
 WHEN 'file.write' THEN ARRAY['path','text','content_base64','append','expected_sha256','mkdirs']
 WHEN 'file.edit' THEN ARRAY['path','expected_sha256','edits']
 WHEN 'file.patch' THEN ARRAY['path','expected_sha256','patch']
 WHEN 'file.delete' THEN ARRAY['path','recursive']
 WHEN 'file.list' THEN ARRAY['path','offset','limit']
 WHEN 'file.stat' THEN ARRAY['path'] WHEN 'file.hash' THEN ARRAY['path'] WHEN 'file.mkdir' THEN ARRAY['path','exist_ok']
 WHEN 'code.search' THEN ARRAY['pattern','glob','regex','ignore_case','hidden','limit']
 WHEN 'code.glob' THEN ARRAY['pattern','hidden','limit']
 WHEN 'git.status' THEN ARRAY[]::text[] WHEN 'git.diff' THEN ARRAY['path','staged']
 WHEN 'workspace.create' THEN ARRAY['name','branch','ref']
 WHEN 'job.status' THEN ARRAY['id'] WHEN 'job.cancel' THEN ARRAY['id'] WHEN 'job.output' THEN ARRAY['id','offset','length','text']
 ELSE ARRAY[]::text[] END;
 FOR k IN SELECT jsonb_object_keys(p) LOOP
  IF NOT k=ANY(allowed) THEN RAISE EXCEPTION 'INVALID_ARGUMENT: unknown field % for %',k,p_kind; END IF;
 END LOOP;
 FOREACH k IN ARRAY ARRAY['workspace','cwd','request_key','path','pattern','glob','branch','ref','name','content_base64','patch','expected_sha256'] LOOP
  IF p?k AND (jsonb_typeof(p->k)<>'string' OR length(p->>k)=0) THEN RAISE EXCEPTION 'INVALID_ARGUMENT: % must be a nonempty string',k; END IF;
 END LOOP;
 IF p?'request_key' AND length(p->>'request_key')>200 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: request_key exceeds 200 characters'; END IF;
 FOREACH k IN ARRAY ARRAY['append','recursive','mkdirs','exist_ok','include_hash','regex','ignore_case','hidden','staged'] LOOP
  IF p?k AND jsonb_typeof(p->k)<>'boolean' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: % must be boolean',k; END IF;
 END LOOP;
 IF p?'text' AND jsonb_typeof(p->'text')<>(CASE WHEN p_kind='file.write' THEN 'string' ELSE 'boolean' END) THEN RAISE EXCEPTION 'INVALID_ARGUMENT: text has wrong type'; END IF;
 FOREACH k IN ARRAY ARRAY['offset','length','start_line','count','limit'] LOOP
  IF p?k THEN
   IF jsonb_typeof(p->k)<>'number' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: % must be an integer',k; END IF;
   n:=(p->>k)::numeric;
   IF n<>trunc(n) OR n<(CASE WHEN k='offset' THEN 0 ELSE 1 END) OR n>(CASE k WHEN 'offset' THEN CASE WHEN p_kind='file.list' THEN 10000 ELSE 9223372036854775807 END WHEN 'length' THEN CASE WHEN p_kind='job.output' THEN 1000000 ELSE 4000000 END WHEN 'count' THEN 2000 WHEN 'limit' THEN 1000 ELSE 2147483648 END) THEN RAISE EXCEPTION 'INVALID_ARGUMENT: % out of range',k; END IF;
  END IF;
 END LOOP;
 IF p_kind LIKE 'file.%' AND NOT p?'path' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: file path required'; END IF;
 IF p_kind='code.search' AND NOT p?'pattern' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: pattern required'; END IF;
 IF p_kind IN ('job.status','job.cancel','job.output') THEN
  IF jsonb_typeof(p->'id') IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: job id required'; END IF;
  PERFORM (p->>'id')::uuid;
 END IF;
 IF p?'expected_sha256' AND (p->>'expected_sha256')!~'^(missing|[a-f0-9]{64})$' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: invalid expected hash'; END IF;
 IF p_kind='file.write' THEN
  IF (p?'text')=(p?'content_base64') THEN RAISE EXCEPTION 'INVALID_ARGUMENT: specify exactly one of text/content_base64'; END IF;
  IF p?'text' AND octet_length(p->>'text')>4000000 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: write exceeds 4 MB'; END IF;
  IF p?'content_base64' AND length(p->>'content_base64')>5333336 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: base64 write exceeds 4 MB'; END IF;
 END IF;
 IF p_kind IN ('file.edit','file.patch') AND NOT p?'expected_sha256' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: expected_sha256 required'; END IF;
 IF p_kind='file.edit' THEN
  IF jsonb_typeof(p->'edits') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: edits array required'; END IF;
  IF jsonb_array_length(p->'edits') NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: 1-100 edits required'; END IF;
  FOR i IN SELECT value FROM jsonb_array_elements(p->'edits') LOOP
   IF jsonb_typeof(i)<>'object' OR jsonb_typeof(i->'old') IS DISTINCT FROM 'string' OR coalesce(length(i->>'old'),0)=0 OR jsonb_typeof(i->'new') IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: each edit needs old and new strings'; END IF;
  END LOOP;
 END IF;
 IF p_kind='file.patch' AND NOT p?'patch' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: patch required'; END IF;
 IF p_kind='workspace.create' AND (NOT p?'name' OR NOT p?'branch') THEN RAISE EXCEPTION 'INVALID_ARGUMENT: name and branch required'; END IF;
END; $f$;
CREATE OR REPLACE FUNCTION public.agent_commands_guard()
RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $f$
DECLARE old_hash bytea;old_id uuid;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.command,NEW.kind,NEW.payload,NEW.timeout_sec,NEW.request_key) IS DISTINCT FROM ROW(OLD.command,OLD.kind,OLD.payload,OLD.timeout_sec,OLD.request_key) THEN RAISE EXCEPTION 'REQUEST_IMMUTABLE: submit a new request; do not edit queued work'; END IF;
  IF OLD.status IN ('done','error') AND NEW.status<>OLD.status THEN RAISE EXCEPTION 'TERMINAL_IMMUTABLE: do not reset completed work'; END IF;
  IF OLD.status='running' AND NEW.status='pending' THEN RAISE EXCEPTION 'REQUEUE_FORBIDDEN: coordinator owns safe-read retries'; END IF;
  IF OLD.executed AND NOT NEW.executed THEN RAISE EXCEPTION 'EXECUTION_MARKER_IMMUTABLE'; END IF;
  RETURN NEW;
 END IF;
 PERFORM public.validate_agent_request(NEW.kind,coalesce(NEW.payload,'{}'));
 IF NEW.kind='shell' AND btrim(NEW.command)='' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: empty shell command'; END IF;
 IF NEW.request_key IS NOT NULL AND NEW.payload?'request_key' AND NEW.request_key IS DISTINCT FROM NEW.payload->>'request_key' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: request_key mismatch'; END IF;
 IF NEW.request_key IS NULL THEN NEW.request_key:=nullif(NEW.payload->>'request_key',''); END IF;
 IF NEW.request_key IS NOT NULL THEN
  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.request_key,0));
  SELECT request_hash,command_id INTO old_hash,old_id FROM public.agent_request_ledger WHERE request_key=NEW.request_key;
  IF FOUND THEN RAISE EXCEPTION 'REQUEST_KEY_EXISTS: use submit_command to recover the original tracking id'; END IF;
  INSERT INTO public.agent_request_ledger(request_key,request_hash,command_id) VALUES(NEW.request_key,public.agent_request_hash(NEW.command,NEW.timeout_sec,NEW.kind,NEW.payload),NEW.id);
 END IF;
 RETURN NEW;
END; $f$;
CREATE TRIGGER agent_commands_guard BEFORE INSERT OR UPDATE ON public.agent_commands FOR EACH ROW EXECUTE FUNCTION public.agent_commands_guard();
CREATE OR REPLACE FUNCTION public.submit_command(p_command text,p_run_timeout_sec integer DEFAULT NULL,p_kind text DEFAULT 'shell',p_payload jsonb DEFAULT NULL)
RETURNS uuid LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $f$
DECLARE v_id uuid;v_key text;v_payload jsonb:=coalesce(p_payload,'{}');v_command text;v_hash bytea;old_hash bytea;
BEGIN
 PERFORM public.validate_agent_request(p_kind,v_payload);
 IF p_run_timeout_sec IS NOT NULL AND p_run_timeout_sec NOT BETWEEN 1 AND 7200 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: runtime must be 1-7200'; END IF;
 IF p_kind='shell' AND (p_command IS NULL OR btrim(p_command)='') THEN RAISE EXCEPTION 'INVALID_ARGUMENT: shell command required'; END IF;
 v_command:=CASE WHEN p_kind='shell' THEN p_command ELSE p_kind END;
 v_key:=nullif(v_payload->>'request_key','');v_hash:=public.agent_request_hash(v_command,p_run_timeout_sec,p_kind,v_payload);
 IF v_key IS NOT NULL THEN
  PERFORM pg_advisory_xact_lock(hashtextextended(v_key,0));
  SELECT command_id,request_hash INTO v_id,old_hash FROM public.agent_request_ledger WHERE request_key=v_key;
  IF FOUND THEN
   IF old_hash<>v_hash THEN RAISE EXCEPTION 'REQUEST_KEY_CONFLICT: same key has different request'; END IF;
   RETURN v_id;
  END IF;
 END IF;
 INSERT INTO public.agent_commands(command,timeout_sec,kind,payload,request_key) VALUES(v_command,p_run_timeout_sec,p_kind,v_payload,v_key) RETURNING id INTO v_id;
 RETURN v_id;
END; $f$;
CREATE INDEX IF NOT EXISTS agent_commands_running_runner_idx ON public.agent_commands(claimed_by,created_at,id) WHERE status='running';
CREATE INDEX IF NOT EXISTS agent_commands_finished_idx ON public.agent_commands(finished_at,id) WHERE status IN ('done','error');
CREATE OR REPLACE FUNCTION public.command_result_v2(p_id uuid,p_wait_seconds integer DEFAULT 20)
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $f$
DECLARE r record;k text;p jsonb;v jsonb;err text;beat timestamptz;hint jsonb;
BEGIN
 SELECT * INTO r FROM public.command_result(p_id,least(20,greatest(0,coalesce(p_wait_seconds,20))));
 IF r.status IS NULL THEN
  RETURN jsonb_build_object('protocol_version','3.1','tracking_id',p_id,'status',CASE WHEN EXISTS(SELECT 1 FROM public.agent_request_ledger WHERE command_id=p_id) THEN 'result_expired' ELSE 'not_found' END,'error_code',CASE WHEN EXISTS(SELECT 1 FROM public.agent_request_ledger WHERE command_id=p_id) THEN 'RESULT_EXPIRED' ELSE 'NOT_FOUND_OR_NOT_VISIBLE' END,'resubmit',false);
 END IF;
 SELECT kind,payload INTO k,p FROM public.agent_commands WHERE id=p_id;
 IF k='shell' THEN v:=to_jsonb(r.result);
 ELSE BEGIN v:=r.result::jsonb;EXCEPTION WHEN invalid_text_representation THEN v:=to_jsonb(r.result);END;END IF;
 IF r.status IN ('pending','running') THEN
  SELECT last_seen INTO beat FROM public.bridge_status WHERE id=1;
  hint:=jsonb_build_object('action','wait_same_id','tracking_id',p_id,'wait_seconds',20,'bridge_heartbeat_age_seconds',extract(epoch FROM clock_timestamp()-beat),'bridge_stale',beat IS NULL OR beat<clock_timestamp()-interval '1 minute');
 ELSIF jsonb_typeof(v)='object' THEN
  IF v?'next_offset' THEN hint:=jsonb_build_object('next_offset',v->'next_offset','has_more',v->'has_more');
  ELSIF v?'next_line' THEN hint:=jsonb_build_object('next_line',v->'next_line','has_more',v->'has_more');
  ELSIF v?'bytes_read' THEN hint:=jsonb_build_object('next_offset',coalesce((v->>'offset')::bigint,0)+(v->>'bytes_read')::bigint,'has_more',v->'has_more');END IF;
 END IF;
 err:=CASE WHEN r.status<>'error' THEN NULL WHEN r.exit_code=124 THEN 'TIMEOUT' WHEN r.exit_code=125 THEN 'OUTCOME_UNCERTAIN' WHEN r.exit_code=126 THEN 'BRIDGE_FAILURE' WHEN r.exit_code=130 THEN 'CANCELLED' ELSE 'OPERATION_FAILED' END;
 RETURN jsonb_build_object('protocol_version','3.1','tracking_id',p_id,'status',r.status,'exit_code',r.exit_code,'duration_ms',r.duration_ms,'kind',k,'data',v,'error_code',err,'partial_effects_possible',r.status='error' AND k IN ('shell','file.write','file.edit','file.patch','file.delete','file.mkdir','workspace.create'),'continuation',hint,'resubmit',false);
END; $f$;
REVOKE ALL ON FUNCTION public.agent_request_hash(text,integer,text,jsonb),public.validate_agent_request(text,jsonb),public.agent_commands_guard() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.agent_request_hash(text,integer,text,jsonb),public.validate_agent_request(text,jsonb),public.agent_commands_guard() TO anon,service_role,authenticated;
GRANT EXECUTE ON FUNCTION public.command_result_v2(uuid,integer) TO anon,authenticated,service_role;
NOTIFY pgrst,'reload schema';
COMMIT;
