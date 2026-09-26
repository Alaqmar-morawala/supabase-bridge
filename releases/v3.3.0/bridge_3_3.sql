BEGIN;
-- supabase-bridge 3.3 schema changes. Apply after bridge_3_2.sql.
-- Additive: git.log, git.show and workspace.remove kinds; progress objects surfaced for running shell jobs.

ALTER TABLE public.agent_commands DROP CONSTRAINT IF EXISTS agent_commands_kind_check;
ALTER TABLE public.agent_commands ADD CONSTRAINT agent_commands_kind_check CHECK (kind = ANY (ARRAY['shell','file.read','file.read_lines','file.write','file.edit','file.patch','file.delete','file.list','file.stat','file.hash','file.mkdir','code.search','code.glob','git.status','git.diff','git.log','git.show','workspace.list','workspace.create','workspace.remove','job.status','job.output','job.cancel','bridge.health','batch']));

CREATE OR REPLACE FUNCTION public.validate_agent_request(p_kind text,p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $f$
DECLARE k text;v jsonb;n numeric;allowed text[];common text[]:=ARRAY['workspace','cwd','request_key','max_queue_age_sec'];i jsonb;
BEGIN
 IF p_kind IS NULL OR p_kind NOT IN ('shell','file.read','file.read_lines','file.write','file.edit','file.patch','file.delete','file.list','file.stat','file.hash','file.mkdir','code.search','code.glob','git.status','git.diff','git.log','git.show','workspace.list','workspace.create','workspace.remove','job.status','job.output','job.cancel','bridge.health','batch') THEN RAISE EXCEPTION 'INVALID_ARGUMENT: unsupported kind'; END IF;
 IF p IS NULL OR jsonb_typeof(p)<>'object' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: payload must be an object'; END IF;
 IF octet_length(p::text)>24000000 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: payload exceeds 24 MB'; END IF;
 allowed:=common || CASE p_kind
 WHEN 'shell' THEN ARRAY['scope']
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
 WHEN 'git.log' THEN ARRAY['path','ref','limit'] WHEN 'git.show' THEN ARRAY['path','ref']
 WHEN 'workspace.create' THEN ARRAY['name','branch','ref']
 WHEN 'workspace.remove' THEN ARRAY['name','force']
 WHEN 'job.status' THEN ARRAY['id'] WHEN 'job.cancel' THEN ARRAY['id'] WHEN 'job.output' THEN ARRAY['id','offset','length','text']
 WHEN 'batch' THEN ARRAY['items']
 ELSE ARRAY[]::text[] END;
 FOR k IN SELECT jsonb_object_keys(p) LOOP
  IF NOT k=ANY(allowed) THEN RAISE EXCEPTION 'INVALID_ARGUMENT: unknown field % for %',k,p_kind; END IF;
 END LOOP;
 FOREACH k IN ARRAY ARRAY['workspace','cwd','request_key','path','pattern','glob','branch','ref','name','content_base64','patch','expected_sha256','scope'] LOOP
  IF p?k AND (jsonb_typeof(p->k)<>'string' OR length(p->>k)=0) THEN RAISE EXCEPTION 'INVALID_ARGUMENT: % must be a nonempty string',k; END IF;
 END LOOP;
 IF p?'scope' AND p->>'scope' NOT IN ('workspace','global') THEN RAISE EXCEPTION 'INVALID_ARGUMENT: scope must be workspace or global'; END IF;
 IF p?'ref' AND left(p->>'ref',1)='-' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: ref must not start with a dash'; END IF;
 IF p?'request_key' AND length(p->>'request_key')>200 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: request_key exceeds 200 characters'; END IF;
 FOREACH k IN ARRAY ARRAY['append','recursive','mkdirs','exist_ok','include_hash','regex','ignore_case','hidden','staged','force'] LOOP
  IF p?k AND jsonb_typeof(p->k)<>'boolean' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: % must be boolean',k; END IF;
 END LOOP;
 IF p?'text' AND jsonb_typeof(p->'text')<>(CASE WHEN p_kind='file.write' THEN 'string' ELSE 'boolean' END) THEN RAISE EXCEPTION 'INVALID_ARGUMENT: text has wrong type'; END IF;
 FOREACH k IN ARRAY ARRAY['offset','length','start_line','count','limit','max_queue_age_sec'] LOOP
  IF p?k THEN
   IF jsonb_typeof(p->k)<>'number' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: % must be an integer',k; END IF;
   n:=(p->>k)::numeric;
   IF n<>trunc(n) OR n<(CASE WHEN k='offset' THEN 0 ELSE 1 END) OR n>(CASE k WHEN 'offset' THEN CASE WHEN p_kind='file.list' THEN 10000 ELSE 9223372036854775807 END WHEN 'length' THEN CASE WHEN p_kind='job.output' THEN 1000000 ELSE 4000000 END WHEN 'count' THEN 2000 WHEN 'limit' THEN CASE WHEN p_kind='git.log' THEN 200 ELSE 1000 END WHEN 'max_queue_age_sec' THEN 86400 ELSE 2147483648 END) THEN RAISE EXCEPTION 'INVALID_ARGUMENT: % out of range',k; END IF;
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
 IF p_kind='workspace.remove' AND NOT p?'name' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: name required'; END IF;
 IF p_kind='batch' THEN
  IF jsonb_typeof(p->'items') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: items array required'; END IF;
  IF jsonb_array_length(p->'items') NOT BETWEEN 1 AND 50 THEN RAISE EXCEPTION 'INVALID_ARGUMENT: 1-50 items required'; END IF;
  FOR i IN SELECT value FROM jsonb_array_elements(p->'items') LOOP
   IF jsonb_typeof(i)<>'object' OR jsonb_typeof(i->'payload') IS DISTINCT FROM 'object' OR (i->>'kind') IS NULL OR (i->>'kind') NOT IN ('file.read','file.read_lines','file.hash','file.stat','file.list','code.search','code.glob','git.status','git.diff','git.log','git.show') THEN RAISE EXCEPTION 'INVALID_ARGUMENT: each batch item needs a safe read kind and an object payload'; END IF;
   IF (i->'payload')?'workspace' OR (i->'payload')?'request_key' OR (i->'payload')?'max_queue_age_sec' OR (i->'payload')?'scope' OR (i->'payload')?'items' THEN RAISE EXCEPTION 'INVALID_ARGUMENT: batch items may not set workspace, request_key, max_queue_age_sec, scope or items'; END IF;
   PERFORM public.validate_agent_request(i->>'kind',i->'payload');
  END LOOP;
 END IF;
END; $f$;

CREATE OR REPLACE FUNCTION public.command_result_v2(p_id uuid,p_wait_seconds integer DEFAULT 20)
RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $f$
DECLARE r record;k text;p jsonb;v jsonb;v2 jsonb;err text;beat timestamptz;hint jsonb;effective_wait integer;limit_ms numeric;started timestamptz;
BEGIN
 effective_wait:=least(20,greatest(0,coalesce(p_wait_seconds,20)));
 SELECT setting::numeric INTO limit_ms FROM pg_settings WHERE name='statement_timeout';
 IF limit_ms>0 THEN effective_wait:=least(effective_wait,greatest(0,floor((limit_ms-1000)/1000)::integer));END IF;
 SELECT * INTO r FROM public.command_result(p_id,effective_wait);
 IF r.status IS NULL THEN
  RETURN jsonb_build_object('protocol_version','3.2','tracking_id',p_id,'status',CASE WHEN EXISTS(SELECT 1 FROM public.agent_request_ledger WHERE command_id=p_id) THEN 'result_expired' ELSE 'not_found' END,'error_code',CASE WHEN EXISTS(SELECT 1 FROM public.agent_request_ledger WHERE command_id=p_id) THEN 'RESULT_EXPIRED' ELSE 'NOT_FOUND_OR_NOT_VISIBLE' END,'resubmit',false);
 END IF;
 SELECT kind,payload,started_at INTO k,p,started FROM public.agent_commands WHERE id=p_id;
 IF k='shell' THEN
  v:=to_jsonb(r.result);
  IF r.status IN ('error','running') AND left(btrim(coalesce(r.result,'')),1)='{' THEN
   BEGIN
    v2:=r.result::jsonb;
    IF jsonb_typeof(v2)='object' AND ((r.status='error' AND v2?'error_code' AND v2?'protocol_version') OR (r.status='running' AND v2?'progress')) THEN v:=v2; END IF;
   EXCEPTION WHEN others THEN NULL; END;
  END IF;
 ELSE BEGIN v:=r.result::jsonb;EXCEPTION WHEN invalid_text_representation THEN v:=to_jsonb(r.result);END;END IF;
 IF r.status IN ('pending','running') THEN
  SELECT last_seen INTO beat FROM public.bridge_status WHERE id=1;
  hint:=jsonb_build_object('action','wait_same_id','tracking_id',p_id,'wait_seconds',effective_wait,'bridge_heartbeat_age_seconds',extract(epoch FROM clock_timestamp()-beat),'bridge_stale',beat IS NULL OR beat<clock_timestamp()-interval '1 minute');
 ELSIF jsonb_typeof(v)='object' THEN
  IF v?'next_offset' THEN hint:=jsonb_build_object('next_offset',v->'next_offset','has_more',v->'has_more');
  ELSIF v?'next_line' THEN hint:=jsonb_build_object('next_line',v->'next_line','has_more',v->'has_more');
  ELSIF v?'bytes_read' THEN hint:=jsonb_build_object('next_offset',coalesce((v->>'offset')::bigint,0)+(v->>'bytes_read')::bigint,'has_more',v->'has_more');END IF;
 END IF;
 err:=CASE WHEN r.status<>'error' THEN NULL WHEN jsonb_typeof(v)='object' AND jsonb_typeof(v->'error_code')='string' THEN v->>'error_code' WHEN r.exit_code=124 THEN 'TIMEOUT' WHEN r.exit_code=125 THEN 'OUTCOME_UNCERTAIN' WHEN r.exit_code=126 THEN 'BRIDGE_FAILURE' WHEN r.exit_code=130 THEN 'CANCELLED' ELSE 'OPERATION_FAILED' END;
 RETURN jsonb_build_object('protocol_version','3.2','tracking_id',p_id,'status',r.status,'exit_code',r.exit_code,'duration_ms',r.duration_ms,'kind',k,'started_at',started,'data',v,'error_code',err,'partial_effects_possible',r.status='error' AND started IS NOT NULL AND k IN ('shell','file.write','file.edit','file.patch','file.delete','file.mkdir','workspace.create','workspace.remove'),'continuation',hint,'resubmit',false);
END; $f$;
NOTIFY pgrst,'reload schema';
COMMIT;
