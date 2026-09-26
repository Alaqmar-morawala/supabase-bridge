BEGIN;
-- supabase-bridge 3.1.1 baseline, captured 26 September 2026 from the live project.
-- This is the command_result_v2 definition that was deployed while bridge_3_1.sql still
-- carried the older fixed-20-second version. Differences: the wait is capped to the
-- caller's statement_timeout with a one-second margin, and the effective wait is reported
-- in the continuation hint. Apply after bridge_3_1.sql and before bridge_3_2.sql.
CREATE OR REPLACE FUNCTION public.command_result_v2(p_id uuid, p_wait_seconds integer DEFAULT 20)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'public'
AS $function$
DECLARE r record;k text;p jsonb;v jsonb;err text;beat timestamptz;hint jsonb;effective_wait integer;limit_ms numeric;
BEGIN
 effective_wait:=least(20,greatest(0,coalesce(p_wait_seconds,20)));
 SELECT setting::numeric INTO limit_ms FROM pg_settings WHERE name='statement_timeout';
 IF limit_ms>0 THEN effective_wait:=least(effective_wait,greatest(0,floor((limit_ms-1000)/1000)::integer));END IF;
 SELECT * INTO r FROM public.command_result(p_id,effective_wait);
 IF r.status IS NULL THEN
  RETURN jsonb_build_object('protocol_version','3.1','tracking_id',p_id,'status',CASE WHEN EXISTS(SELECT 1 FROM public.agent_request_ledger WHERE command_id=p_id) THEN 'result_expired' ELSE 'not_found' END,'error_code',CASE WHEN EXISTS(SELECT 1 FROM public.agent_request_ledger WHERE command_id=p_id) THEN 'RESULT_EXPIRED' ELSE 'NOT_FOUND_OR_NOT_VISIBLE' END,'resubmit',false);
 END IF;
 SELECT kind,payload INTO k,p FROM public.agent_commands WHERE id=p_id;
 IF k='shell' THEN v:=to_jsonb(r.result);
 ELSE BEGIN v:=r.result::jsonb;EXCEPTION WHEN invalid_text_representation THEN v:=to_jsonb(r.result);END;END IF;
 IF r.status IN ('pending','running') THEN
  SELECT last_seen INTO beat FROM public.bridge_status WHERE id=1;
  hint:=jsonb_build_object('action','wait_same_id','tracking_id',p_id,'wait_seconds',effective_wait,'bridge_heartbeat_age_seconds',extract(epoch FROM clock_timestamp()-beat),'bridge_stale',beat IS NULL OR beat<clock_timestamp()-interval '1 minute');
 ELSIF jsonb_typeof(v)='object' THEN
  IF v?'next_offset' THEN hint:=jsonb_build_object('next_offset',v->'next_offset','has_more',v->'has_more');
  ELSIF v?'next_line' THEN hint:=jsonb_build_object('next_line',v->'next_line','has_more',v->'has_more');
  ELSIF v?'bytes_read' THEN hint:=jsonb_build_object('next_offset',coalesce((v->>'offset')::bigint,0)+(v->>'bytes_read')::bigint,'has_more',v->'has_more');END IF;
 END IF;
 err:=CASE WHEN r.status<>'error' THEN NULL WHEN r.exit_code=124 THEN 'TIMEOUT' WHEN r.exit_code=125 THEN 'OUTCOME_UNCERTAIN' WHEN r.exit_code=126 THEN 'BRIDGE_FAILURE' WHEN r.exit_code=130 THEN 'CANCELLED' ELSE 'OPERATION_FAILED' END;
 RETURN jsonb_build_object('protocol_version','3.1','tracking_id',p_id,'status',r.status,'exit_code',r.exit_code,'duration_ms',r.duration_ms,'kind',k,'data',v,'error_code',err,'partial_effects_possible',r.status='error' AND k IN ('shell','file.write','file.edit','file.patch','file.delete','file.mkdir','workspace.create'),'continuation',hint,'resubmit',false);
END; $function$;
COMMIT;
