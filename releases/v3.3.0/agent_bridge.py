#!/usr/bin/env python3
"""Bridge 3.3: reconnectable local jobs, safe retries, adaptive polling, scoped locks, dispatch expiry and batch reads. No Blocked state."""
import os,sys,json,time,signal,hashlib,base64,subprocess,fcntl,socket,shutil,uuid,threading,tempfile,traceback,re
from pathlib import Path
from datetime import datetime,timezone,timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote
import bridge_lib as core
import workspace_ops as ops
VERSION='supabase-bridge 3.3'
PROTOCOL='3.2'
HOME=Path(os.environ.get('SF_BRIDGE_HOME',str(Path.home()/'supabase-bridge'))).resolve()
JOBS=HOME/'jobs'; CONFIG=HOME/'config.json'; STOP=False
CONTROLS={'job.status','job.output','job.cancel','bridge.health','workspace.list'}
GLOBAL_LOCK='global'
ERROR_CATEGORIES={124:'TIMEOUT',125:'OUTCOME_UNCERTAIN',126:'BRIDGE_FAILURE',130:'CANCELLED'}
RELEASE_DIR=Path(__file__).resolve().parent
core.BRIDGE_DIR=HOME; core.CONFIG_PATH=CONFIG; core.ENV_PATH=HOME/'.env'; core.STATE_PATH=HOME/'state.json'; core.LOG_PATH=HOME/'bridge.log'; core.VERSION=VERSION

def now(): return datetime.now(timezone.utc).isoformat()
def write(path,body): core.atomic_write(Path(path),json.dumps(body,ensure_ascii=True).encode())
def read(path,default=None):
 try: return json.loads(Path(path).read_text())
 except (OSError,ValueError): return default

def release_info():
 files={}
 for name in ('agent_bridge.py','workspace_ops.py','bridge_lib.py'):
  try: files[name]=hashlib.sha256((RELEASE_DIR/name).read_bytes()).hexdigest()
  except OSError: files[name]=None
 return {'path':str(RELEASE_DIR),'version':VERSION,'protocol_version':PROTOCOL,'files':files}
RELEASE_INFO=release_info()
EXPECTED_SCHEMA=read(RELEASE_DIR/'expected_schema.json',{})

def cfg_load():
 cfg=read(CONFIG)
 if not cfg: raise ValueError('missing bridge configuration')
 cfg['workspaces']=dict(cfg['workspaces']); cfg['workspaces'].update(read(HOME/'workspaces.json',{}))
 if cfg['default_workspace'] not in cfg['workspaces']: raise ValueError('default workspace missing')
 if type(cfg.get('max_parallel_jobs')) is not int or not 1<=cfg['max_parallel_jobs']<=8:raise ValueError('max_parallel_jobs must be 1-8')
 for key,lo,hi in [('poll_interval_sec',1,60),('exec_timeout_sec',1,7200),('job_tasks_max',16,65536),('output_max_chars',1000,2000000)]:
  if type(cfg.get(key)) is not int or not lo<=cfg[key]<=hi:raise ValueError('invalid config '+key)
 if not isinstance(cfg.get('job_memory_max'),str) or not re.fullmatch(r'[1-9][0-9]*[KMGT]?',cfg['job_memory_max']):raise ValueError('invalid job_memory_max')
 cfg.setdefault('active_poll_interval_sec',0.25)
 active=cfg['active_poll_interval_sec']
 if isinstance(active,bool) or not isinstance(active,(int,float)) or not 0.1<=active<=cfg['poll_interval_sec']:raise ValueError('active_poll_interval_sec must be a number in [0.1, poll_interval_sec]')
 for key,default in [('max_queue_age_sec_safe',3600),('max_queue_age_sec_write',900)]:
  cfg.setdefault(key,default)
  if type(cfg.get(key)) is not int or not 1<=cfg[key]<=86400:raise ValueError('invalid config '+key)
 return cfg

def workspace(row,cfg):
 p=row.get('payload') or {}; name=p.get('workspace',cfg['default_workspace'])
 if name not in cfg['workspaces']: raise ValueError('unknown workspace '+str(name))
 root=Path(cfg['workspaces'][name]).expanduser().resolve()
 cwd=Path(p.get('cwd','.' )).expanduser()
 cwd=(root/cwd).resolve() if not cwd.is_absolute() else cwd.resolve()
 if not cwd.is_dir(): raise ValueError('working directory does not exist')
 # Locks are computed by lock_plan. Shell without a declared scope and absolute
 # paths outside every workspace hold the global exclusive lock.
 return name,str(root),str(cwd)

def workspace_for_path(path,cfg):
 try: target=Path(path).expanduser().resolve()
 except (OSError,RuntimeError,ValueError): return None
 best=None; best_depth=-1
 for name,root in cfg['workspaces'].items():
  try: r=Path(root).expanduser().resolve()
  except (OSError,RuntimeError,ValueError): continue
  if target==r or r in target.parents:
   depth=len(r.parts)
   if depth>best_depth: best=name; best_depth=depth
 return best

def lock_plan(row,cfg,cwd):
 # Readers share their workspace lock; writers hold it exclusively. Work whose
 # extent is unknown (undeclared shell, paths outside every workspace, worktree
 # creation) holds the global lock exclusively. Every job holds the global lock
 # at least shared, so a global writer excludes everything.
 kind=row['kind']; p=row.get('payload') or {}; safe=kind in ops.SAFE_KINDS
 if kind=='shell' and p.get('scope')!='workspace': return [(GLOBAL_LOCK,True)]
 if kind in ('workspace.create','workspace.remove'): return [(GLOBAL_LOCK,True)]
 def resolve(base,raw):
  if not isinstance(raw,str) or not raw: return str(base)
  candidate=Path(raw).expanduser()
  return str(candidate) if candidate.is_absolute() else str(Path(base)/candidate)
 targets=[]
 if kind=='batch':
  for item in p.get('items') or []:
   ip=item.get('payload') or {}; base=resolve(cwd,ip.get('cwd'))
   targets.append(resolve(base,ip.get('path')) if str(item.get('kind','')).startswith('file.') else base)
 elif kind.startswith('file.'): targets.append(resolve(cwd,p.get('path')))
 else: targets.append(str(cwd))
 names=set()
 for target in targets:
  name=workspace_for_path(target,cfg)
  if name is None: return [(GLOBAL_LOCK,not safe)]
  names.add(name)
 return [(GLOBAL_LOCK,False)]+[('ws:'+name,not safe) for name in sorted(names)]

def lock_path(name):
 lockdir=HOME/'locks'
 if name==GLOBAL_LOCK: return lockdir/'workspace-access.lock'
 return lockdir/('ws-'+hashlib.sha1(str(name).encode()).hexdigest()[:16]+'.lock')

def request_locks(request):
 locks=request.get('locks')
 if locks: return [(name,bool(exclusive)) for name,exclusive in locks]
 kind=(request.get('row') or {}).get('kind')
 return [(GLOBAL_LOCK,kind not in ops.SAFE_KINDS)]

def conflicts(plan,live):
 held={}
 for request in live:
  for name,exclusive in request_locks(request):
   held[name]=held.get(name,False) or exclusive
 for name,exclusive in plan:
  if name in held and (exclusive or held[name]): return True
 return False

def queue_age_limit(row,cfg):
 p=row.get('payload') or {}
 limit=cfg.get('max_queue_age_sec_safe',3600) if row.get('kind') in ops.SAFE_KINDS else cfg.get('max_queue_age_sec_write',900)
 override=p.get('max_queue_age_sec')
 if type(override) is int and override>=1: limit=min(limit,override)
 return limit

def parse_time(value):
 if not value: return None
 try: parsed=datetime.fromisoformat(str(value).replace('Z','+00:00'))
 except ValueError: return None
 return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

def expired_before_dispatch(row,cfg,server_now):
 created=parse_time(row.get('created_at'))
 if created is None: return False
 return (server_now-created).total_seconds()>queue_age_limit(row,cfg)

def next_delay(cfg,activity,streak):
 if streak: return min(cfg.get('poll_interval_sec',3)*(2**min(streak,4)),60)
 return float(cfg.get('active_poll_interval_sec',0.25)) if activity else float(cfg.get('poll_interval_sec',3))

def jobdir(cid):
 cid=str(uuid.UUID(str(cid))); return JOBS/cid

def unit_name(cid,attempt): return 'sf-local-job-'+str(uuid.UUID(cid))+'-'+str(attempt)

def unit_state(unit):
 if not isinstance(unit,str) or not unit.startswith('sf-local-job-'):raise ValueError('invalid job unit')
 try:p=subprocess.run(['systemctl','--user','show',unit,'--property=LoadState','--property=ActiveState'],capture_output=True,text=True,timeout=5)
 except (OSError,subprocess.TimeoutExpired):return 'unknown'
 values=dict(line.split('=',1) for line in p.stdout.splitlines() if '=' in line)
 if values.get('LoadState')=='not-found':return 'missing'
 if p.returncode!=0:return 'unknown'
 return values.get('ActiveState','unknown')

def active_unit(request):
 # An unavailable systemd status is NOT proof of a dead worker.
 if not request:return False
 return unit_state(request['unit']) not in ('missing','inactive','failed')

def reply_error(code,message,error_code=None,partial=True):
 category=error_code or ERROR_CATEGORIES.get(code,'OPERATION_FAILED')
 return {'exit_code':code,'result':json.dumps({'error':message,'error_code':category,'protocol_version':PROTOCOL,'partial_effects_possible':bool(partial)}),'duration_ms':0,'finished_at':now(),'total_bytes':0}

def error_code_of(meta):
 try:
  body=json.loads(meta.get('result') or '')
  if isinstance(body,dict) and isinstance(body.get('error_code'),str): return body['error_code']
 except (ValueError,TypeError): pass
 return ERROR_CATEGORIES.get(meta.get('exit_code'),'OPERATION_FAILED')

def worker(cid):
 directory=jobdir(cid); request=read(directory/'request.json')
 if not request: raise RuntimeError('missing durable job request')
 row=request['row']; cfg=request['config']; kind=row['kind']; payload=row.get('payload') or {}; timeout=row.get('timeout_sec') or cfg['exec_timeout_sec']
 started=time.monotonic(); cancel=directory/'cancel.json'; locks=[]; meta=None; lease_stop=threading.Event()
 core.OUTPUT_DIR=directory; core.CURRENT_CHILD=None; core.STOP_REQUESTED=False
 def stopping(signo,frame):
  core.STOP_REQUESTED=True
  child=core.CURRENT_CHILD
  if child is not None: core.kill_proc_group(child,signal.SIGTERM)
 signal.signal(signal.SIGTERM,stopping); signal.signal(signal.SIGINT,stopping)
 phase=['waiting_for_workspace_lock']
 def lease():
  while not lease_stop.is_set():
   try: write(directory/'lease.json',{'pid':os.getpid(),'updated_at':now(),'phase':phase[0],'attempt':request['attempt']})
   except OSError: pass
   lease_stop.wait(5)
 thread=threading.Thread(target=lease,daemon=True); thread.start()
 try:
  lockdir=HOME/'locks'; lockdir.mkdir(parents=True,exist_ok=True,mode=0o700)
  plan=request.get('locks')
  if not plan:
   try: plan=lock_plan(row,cfg,request['cwd'])
   except Exception: plan=[[GLOBAL_LOCK,True]]
  for name,exclusive in plan:
   phase[0]='waiting_for_lock:'+str(name)
   lock=open(lock_path(name),'a'); locks.append(lock)
   mode=fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
   while True:
    if cancel.exists() or core.STOP_REQUESTED: raise InterruptedError('cancelled before execution')
    if time.monotonic()-started>=timeout: raise TimeoutError('runtime expired while waiting for workspace access')
    try: fcntl.flock(lock.fileno(),mode|fcntl.LOCK_NB); break
    except BlockingIOError: time.sleep(0.1)
  remaining=max(0.01,timeout-(time.monotonic()-started))
  phase[0]='executing'
  write(directory/'started.json',{'started_at':now(),'pid':os.getpid(),'attempt':request['attempt'],'cwd':request['cwd'],'locks':[[name,bool(exclusive)] for name,exclusive in plan]})
  if kind=='shell':
   execution_cfg={'workdir':request['cwd'],'output_max_chars':cfg['output_max_chars']}
   meta,head,tail=core.execute(execution_cfg,cid,row['command'],remaining,lambda:None)
   meta['result']=core.inline_result(meta,head,tail)
  else:
   body=ops.perform(kind,payload,request['cwd'],HOME,cfg)
   text=json.dumps(body,ensure_ascii=True)
   if len(text.encode())>32_000_000: raise ValueError('result exceeds 32 MB; use smaller pages')
   exit_code=1 if isinstance(body,dict) and body.get('error_code') else 0
   meta={'exit_code':exit_code,'result':text,'total_bytes':len(text.encode()),'duration_ms':int((time.monotonic()-started)*1000),'finished_at':now()}
  if cancel.exists() or core.STOP_REQUESTED: meta=reply_error(130,'job cancelled; partial effects are possible')
 except InterruptedError as e: meta=reply_error(130,str(e))
 except TimeoutError as e: meta=reply_error(124,str(e))
 except Exception as e: meta=reply_error(1,type(e).__name__+': '+str(e)[:4000])
 finally:
  try:
   if meta is not None:
    meta['duration_ms']=int((time.monotonic()-started)*1000);meta['attempt']=request['attempt']
    write(directory/'result.json',meta)
  finally:
   for lock in locks:lock.close()
   lease_stop.set();thread.join(timeout=1)

def start_job(row,cfg,attempt=1,plan=None):
 cid=row['id']; directory=jobdir(cid); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
 if (directory/'cancel.json').exists():raise InterruptedError('cancelled before launch')
 name,root,cwd=workspace(row,cfg)
 ops.validate_payload(row['kind'],row.get('payload') or {})
 if plan is None: plan=lock_plan(row,cfg,cwd)
 request={'row':row,'config':cfg,'workspace':name,'root':root,'cwd':cwd,'attempt':attempt,'unit':unit_name(cid,attempt),'submitted_at':now(),'release':str(Path(__file__).parent),'locks':[[lock,bool(exclusive)] for lock,exclusive in plan]}
 # Persist launch intent first. An absent unit after uncertain launch is never
 # used as permission to rerun writes.
 write(directory/'request.json',request)
 deadline=(row.get('timeout_sec') or cfg['exec_timeout_sec'])+15
 args=['systemd-run','--user','--collect','--unit='+request['unit'],'--property=Type=exec','--property=KillMode=control-group','--property=TimeoutStopSec=5','--property=RuntimeMaxSec='+str(deadline),'--property=MemoryMax='+cfg.get('job_memory_max','2G'),'--property=TasksMax='+str(cfg.get('job_tasks_max',512)),'--property=UMask=0077','--setenv=SF_BRIDGE_HOME='+str(HOME),sys.executable,str(Path(__file__).resolve()),'--worker',cid]
 if (directory/'cancel.json').exists():raise InterruptedError('cancelled before launch')
 p=subprocess.run(args,capture_output=True,text=True,timeout=10)
 if p.returncode:raise RuntimeError('job launch failed: '+p.stderr[:2000])
 if (directory/'cancel.json').exists():
  subprocess.run(['systemctl','--user','stop','--no-block',request['unit']],capture_output=True,text=True,timeout=5)
 return request

def recover_action(row,request,alive,meta,cancelled):
 if alive:return 'adopt'
 if meta is not None:return 'deliver'
 if cancelled: return 'cancel'
 attempt=request.get('attempt',1) if request else 0
 if row['kind'] in ops.SAFE_KINDS and attempt<3: return 'retry'
 return 'uncertain'

class DB(core.Supabase):
 def rest(self,*args,**kwargs):
  core.sd_notify('WATCHDOG=1')
  return super().rest(*args,**kwargs)
 def rows(self,filter): return self.rest('GET','/rest/v1/agent_commands?'+filter)[1]
 def running(self):
  rows=[];offset=0
  while True:
   page=self.rows('status=eq.running&claimed_by=eq.'+quote(socket.gethostname(),safe='')+'&order=created_at.asc,id.asc&limit=100&offset='+str(offset)+'&select=*')
   rows.extend(page)
   if len(page)<100:break
   offset+=len(page)
   if offset>=1000:raise RuntimeError('more than 1000 running claims; dispatch paused for reconciliation')
  return rows
 def pending(self,control=False):
  values=','.join(sorted(CONTROLS)); operator='in' if control else 'not.in'
  return self.rows('status=eq.pending&kind='+operator+'.('+values+')&order=created_at.asc,id.asc&limit=40&select=*')
 def count_pending(self):
  values=','.join(sorted(CONTROLS))
  self.rest('GET','/rest/v1/agent_commands?status=eq.pending&kind=not.in.('+values+')&select=id&limit=1',prefer='count=exact')
  content_range=(getattr(self,'last_headers',None) or {}).get('content-range','')
  total=content_range.rsplit('/',1)[-1] if '/' in content_range else ''
  return int(total) if total.isdigit() else None
 def server_time(self):
  value=(getattr(self,'last_headers',None) or {}).get('date')
  if value:
   try: return parsedate_to_datetime(value).astimezone(timezone.utc)
   except (TypeError,ValueError,IndexError): pass
  return datetime.now(timezone.utc)
 def finish_job(self,cid,meta,executed=True):
  fields={'status':'done' if meta['exit_code']==0 else 'error','result':meta['result'],'exit_code':meta['exit_code'],'duration_ms':meta.get('duration_ms'),'executed':bool(executed),'finished_at':meta.get('finished_at',now())}
  ok=self.finish(cid,fields)
  if not ok:
   # A lost successful PATCH response must be adopted, not retried as work.
   rows=self.rows('id=eq.'+cid+'&select=status,exit_code,result')
   ok=bool(rows and rows[0]['status']==fields['status'] and rows[0]['exit_code']==fields['exit_code'] and rows[0]['result']==fields['result'])
  if ok:
   first=not (jobdir(cid)/'delivered.json').exists()
   try:write(jobdir(cid)/'delivered.json',{'delivered_at':now()})
   except OSError:pass # Cleanup positively checks terminal DB state again.
   self.delivered_in_pass=getattr(self,'delivered_in_pass',0)+1
   if first and hasattr(self,'stats'):
    key='done' if meta['exit_code']==0 else 'error';self.stats[key]=self.stats.get(key,0)+1
    self.stats['last_finished_at']=fields['finished_at']
    if key=='error':
     code=error_code_of(meta);counts=self.stats.setdefault('errors_by_code',{});counts[code]=counts.get(code,0)+1
  return ok

def cancel_job(cid,db):
 directory=jobdir(cid);rows=db.rows('id=eq.'+cid+'&select=id,status')
 if not rows:raise ValueError('job not found')
 status=rows[0]['status']
 if status in ('done','error'):return {'id':cid,'status':status,'cancel_requested':False,'reason':'already terminal'}
 directory.mkdir(parents=True,exist_ok=True,mode=0o700)
 meta=read(directory/'result.json')
 if meta is not None:
  db.finish_job(cid,meta);return {'id':cid,'cancel_requested':False,'reason':'completion receipt already exists'}
 write(directory/'cancel.json',{'requested_at':now()})
 request=read(directory/'request.json')
 if request:
  status=unit_state(request['unit'])
  if status=='unknown':return {'id':cid,'cancel_requested':True,'confirmed_stopped':False,'reason':'worker status unavailable; cancellation marker retained'}
  stopped=subprocess.run(['systemctl','--user','stop','--no-block',request['unit']],capture_output=True,text=True,timeout=5)
  return {'id':cid,'cancel_requested':True,'confirmed_stopped':False,'signal_accepted':stopped.returncode==0}
 meta=reply_error(130,'cancelled before job launch');write(directory/'result.json',meta);db.finish_job(cid,meta)
 return {'id':cid,'cancel_requested':True,'confirmed_stopped':True}

def control(row,db,cfg):
 p=row.get('payload') or {}; kind=row['kind']
 ops.validate_payload(kind,p)
 if kind=='bridge.health':
  stats=getattr(db,'stats',None); stats=dict(stats) if isinstance(stats,dict) else {}
  return {'protocol_version':PROTOCOL,'version':VERSION,'release':RELEASE_INFO,'resource_limits':{'job_memory_max':cfg['job_memory_max'],'job_tasks_max':cfg['job_tasks_max']},'request_keys':'retained independently of job-result cleanup','free_bytes':shutil.disk_usage(HOME).free,'max_parallel_jobs':cfg['max_parallel_jobs'],'active_poll_interval_sec':cfg.get('active_poll_interval_sec',0.25),'max_queue_age_sec_safe':cfg.get('max_queue_age_sec_safe',3600),'max_queue_age_sec_write':cfg.get('max_queue_age_sec_write',900),'default_workspace':cfg['default_workspace'],'workspaces':cfg['workspaces'],'stats':stats}
 if kind=='workspace.list': return ops.perform(kind,p,str(HOME),HOME,cfg)
 cid=str(uuid.UUID(str(p.get('id','')))); directory=jobdir(cid)
 if kind=='job.cancel': return cancel_job(cid,db)
 rows=db.rows('id=eq.'+cid+'&select=id,status,exit_code,duration_ms,started_at,finished_at')
 if not rows: raise ValueError('job not found')
 if kind=='job.status': return dict(rows[0],protocol_version=PROTOCOL,diagnostic=('queued_waiting_for_dispatch' if rows[0]['status']=='pending' else 'completion_saved_delivery_pending' if (directory/'result.json').exists() and rows[0]['status']=='running' else rows[0]['status']),lease=read(directory/'lease.json'),attempt=(read(directory/'request.json') or {}).get('attempt'),locks=(read(directory/'request.json') or {}).get('locks'),output_bytes=(directory/(cid+'.log')).stat().st_size if (directory/(cid+'.log')).exists() else 0)
 if kind=='job.output':
  offset=ops.integer(p,'offset',0,0,2**63-1); length=ops.integer(p,'length',100000,1,1000000); path=directory/(cid+'.log')
  size=path.stat().st_size if path.exists() else 0
  with path.open('rb') if path.exists() else open(os.devnull,'rb') as f: f.seek(offset); data=f.read(length)
  body={'id':cid,'status':rows[0]['status'],'offset':offset,'next_offset':offset+len(data),'has_more':offset+len(data)<size,'eof':rows[0]['status'] in ('done','error') and offset+len(data)>=size,'size':size}
  if p.get('text',True): body['text']=data.decode('utf-8','replace')
  else: body['base64']=base64.b64encode(data).decode()
  return body
 raise ValueError('unknown control')

def reconcile(db,cfg):
 live=[]
 for row in db.running():
  directory=jobdir(row['id']); request=read(directory/'request.json'); meta=read(directory/'result.json')
  alive=active_unit(request)
  if meta is None and not alive: meta=read(directory/'result.json')
  action=recover_action(row,request,alive,meta,(directory/'cancel.json').exists())
  if action=='deliver':
   if not db.finish_job(row['id'],meta):pass
  elif action=='adopt':
   live.append(request)
   if (directory/'cancel.json').exists() and unit_state(request['unit'])!='unknown':
    subprocess.run(['systemctl','--user','stop','--no-block',request['unit']],capture_output=True,text=True,timeout=5)
  elif action=='retry':
   try:
    if row['kind'] in CONTROLS:
     meta={'exit_code':0,'result':json.dumps(control(row,db,cfg)),'duration_ms':0,'finished_at':now()}; directory.mkdir(parents=True,exist_ok=True); write(directory/'result.json',meta); db.finish_job(row['id'],meta)
    else:
     try: plan=lock_plan(row,cfg,workspace(row,cfg)[2])
     except Exception: plan=[(GLOBAL_LOCK,False)]
     if len(live)>=cfg['max_parallel_jobs'] or conflicts(plan,live):continue
     live.append(start_job(row,cfg,(request or {}).get('attempt',0)+1))
   except Exception as e:
    meta=reply_error(126,'safe-read retry could not launch: '+str(e)); write(directory/'result.json',meta); db.finish_job(row['id'],meta)
  else:
   if action=='cancel': meta=reply_error(130,'job cancelled; partial effects are possible')
   else:
    timed_out=False
    if request:
     elapsed=time.time()-datetime.fromisoformat(request['submitted_at']).timestamp()
     timed_out=elapsed>=(row.get('timeout_sec') or cfg['exec_timeout_sec'])
    meta=reply_error(124 if timed_out else 125,'job ended without a durable result; possible partial effects. It was NOT rerun.' if action=='uncertain' else action)
   directory.mkdir(parents=True,exist_ok=True,mode=0o700); write(directory/'result.json',meta); db.finish_job(row['id'],meta)
 return live

def cleanup(db):
 cutoff=time.time()-7*86400
 # Round-robin, bounded inspection prevents an old unknown artifact starving
 # later cleanup candidates. No bulk absence is treated as row deletion.
 cursor=read(HOME/'cleanup-cursor.json',{}).get('last','')
 entries=[]
 for path in JOBS.iterdir():
  if path.is_dir() and not path.is_symlink():
   try:uuid.UUID(path.name);entries.append(path)
   except ValueError:continue
 entries.sort(key=lambda p:p.name)
 ordered=[p for p in entries if p.name>cursor]+[p for p in entries if p.name<=cursor]
 checked=0
 for directory in ordered[:50]:
  checked+=1;cursor=directory.name
  delivered=read(directory/'delivered.json');meta=read(directory/'result.json')
  stamp=(delivered or {}).get('delivered_at') or (meta or {}).get('finished_at')
  try:old=bool(stamp and datetime.fromisoformat(stamp).timestamp()<cutoff)
  except (ValueError,TypeError):old=False
  if not old:continue
  request=read(directory/'request.json')
  if active_unit(request):continue
  rows=db.rows('id=eq.'+directory.name+'&select=id,status,finished_at')
  if rows:
   row=rows[0]
   if row['status'] not in ('done','error') or not row.get('finished_at'):continue
   if datetime.fromisoformat(row['finished_at']).timestamp()>=cutoff:continue
   # Durable intent lets cleanup finish after DELETE commits but its reply is lost.
   write(directory/'cleanup.json',{'terminal_verified_at':now(),'job_id':directory.name})
   code,deleted=db.rest('DELETE','/rest/v1/agent_commands?id=eq.'+directory.name+'&status=in.(done,error)&select=id')
   if code not in (200,204):continue
   # Empty deletion may be another client's deletion. Explicit reread is needed.
   if not deleted and db.rows('id=eq.'+directory.name+'&select=id'):continue
  elif not (directory/'cleanup.json').exists():continue
  shutil.rmtree(directory)
 if checked:write(HOME/'cleanup-cursor.json',{'last':cursor})
 # Legacy outputs and fallback receipts get the same acknowledged-delete order.
 locations=[HOME/'outputs',Path.home()/'.local/share/supabase-bridge/receipts',Path('/tmp/supabase-bridge-receipts')]
 grouped={}
 for location in locations:
  if not location.exists():continue
  for item in location.iterdir():
   if item.is_symlink() or item.suffix not in ('.json','.log','.intent') or not item.is_file():continue
   try:uuid.UUID(item.stem)
   except ValueError:continue
   grouped.setdefault(item.stem,[]).append(item)
 legacy=sorted(cid for cid,paths in grouped.items() if all(p.stat().st_mtime<cutoff for p in paths) and not jobdir(cid).exists())[:50]
 for cid in legacy:
  rows=db.rows('id=eq.'+cid+'&select=id,status,finished_at')
  if not rows or rows[0]['status'] not in ('done','error') or not rows[0].get('finished_at'):continue
  if datetime.fromisoformat(rows[0]['finished_at']).timestamp()>=cutoff:continue
  code,deleted=db.rest('DELETE','/rest/v1/agent_commands?id=eq.'+cid+'&status=in.(done,error)&select=id')
  if code in (200,204) and (deleted or not db.rows('id=eq.'+cid+'&select=id')):
   for artifact in grouped[cid]:artifact.unlink(missing_ok=True)

def check_schema(db,stats):
 expected=(EXPECTED_SCHEMA or {}).get('objects') or {}
 if not expected:
  stats['schema_mismatch']=[]; stats['schema_check']='no expected_schema.json in release'; return
 try: actual=db.rpc('bridge_schema_fingerprint',{})
 except core.BridgeError as e:
  stats['schema_check']='error: '+str(e)[:200]; return
 if isinstance(actual,list): actual=actual[0] if actual else {}
 if not isinstance(actual,dict): actual={}
 mismatch=sorted(name for name in set(expected)|set(actual) if expected.get(name)!=actual.get(name))
 stats['schema_mismatch']=mismatch; stats['schema_check']='ok' if not mismatch else 'mismatch'; stats['schema_checked_at']=now()
 if mismatch: core.log('schema fingerprint mismatch: '+', '.join(mismatch)[:800])

def exclusive_job(live,cfg):
 for request in live:
  exclusive=[name for name,flag in request_locks(request) if flag]
  if not exclusive: continue
  row=request.get('row') or {}; started=parse_time(request.get('submitted_at')); timeout=row.get('timeout_sec') or cfg.get('exec_timeout_sec')
  deadline=(started+timedelta(seconds=timeout)).isoformat() if started and timeout else None
  return {'id':row.get('id'),'kind':row.get('kind'),'started_at':request.get('submitted_at'),'deadline_at':deadline,'locks':exclusive}
 return None

def update_stats(stats,cfg,db,live,remaining,server_now):
 stats['version']=VERSION; stats['protocol_version']=PROTOCOL; stats['active_jobs']=len(live)
 try: stats['queue_depth']=db.count_pending()
 except Exception: stats['queue_depth']=None
 oldest=None
 for row in remaining:
  created=parse_time(row.get('created_at'))
  if created is not None: oldest=max(0,int((server_now-created).total_seconds())); break
 stats['oldest_pending_age_sec']=oldest
 stats['current_exclusive_job']=exclusive_job(live,cfg)
 stats['free_bytes']=shutil.disk_usage(HOME).free
 stats['release']=RELEASE_INFO
 stats.setdefault('errors_by_code',{}); stats.setdefault('last_finished_at',None); stats.setdefault('schema_mismatch',[])
 return stats

def publish_progress(db,live,cache):
 # Publish a small progress object into the running row's result column so
 # command_result_v2 shows phase and output size without a control request.
 # finish_job overwrites it with the final result.
 for request in live:
  row=request.get('row') or {}; cid=row.get('id')
  if not cid: continue
  directory=jobdir(cid); lease=read(directory/'lease.json') or {}
  log=directory/(str(cid)+'.log'); size=log.stat().st_size if log.exists() else 0
  key=(lease.get('phase'),size); last=cache.get(cid); moment=time.monotonic()
  if last and (moment-last[1]<5 or (last[0]==key and moment-last[1]<30)): continue
  body={'progress':{'phase':lease.get('phase'),'attempt':request.get('attempt'),'started_at':request.get('submitted_at'),'output_bytes':size,'locks':request.get('locks'),'updated_at':now(),'protocol_version':PROTOCOL}}
  try: db.rest('PATCH','/rest/v1/agent_commands?id=eq.'+str(cid)+'&status=eq.running&select=id',{'result':json.dumps(body)})
  except core.BridgeError: continue
  cache[cid]=(key,moment)
 active={(request.get('row') or {}).get('id') for request in live}
 for cid in [cid for cid in cache if cid not in active]: cache.pop(cid,None)

def daemon():
 global STOP
 HOME.mkdir(parents=True,exist_ok=True); JOBS.mkdir(exist_ok=True,mode=0o700)
 lock=open(HOME/'.daemon.lock','a')
 try: fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError: raise SystemExit('another bridge daemon is running')
 def stopping(*a):
  global STOP; STOP=True
 signal.signal(signal.SIGTERM,stopping); signal.signal(signal.SIGINT,stopping)
 cfg=cfg_load(); url,key,secret=core.load_creds(); db=DB(cfg,url,key,secret)
 state=read(HOME/'state.json',{'stats':{'done':0,'error':0}}); stats=state.setdefault('stats',{}); stats.pop('blocked',None); db.stats=stats
 state['version']=VERSION; state['release']=str(RELEASE_DIR)
 streak=0; last_clean=0; last_beat=0.0; last_schema=0.0; progress_cache={}
 core.log('coordinator started: '+VERSION+' from '+str(RELEASE_DIR))
 core.sd_notify('READY=1')
 while not STOP:
  core.sd_notify('WATCHDOG=1'); activity=False; db.delivered_in_pass=0; live=[]
  try:
   cfg=cfg_load()
   if last_schema==0.0 or time.monotonic()-last_schema>=3600: check_schema(db,stats); last_schema=time.monotonic()
   # Control lane always progresses, even when normal job slots are full.
   for row in db.pending(True)[:8]:
    if not db.claim(row['id'],socket.gethostname()): continue
    activity=True
    directory=jobdir(row['id']); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    try: meta={'exit_code':0,'result':json.dumps(control(row,db,cfg)),'duration_ms':0,'finished_at':now()}
    except Exception as e: meta=reply_error(1,type(e).__name__+': '+str(e))
    write(directory/'result.json',meta); db.finish_job(row['id'],meta)
   live=reconcile(db,cfg)
   pending=db.pending(False); server_now=db.server_time(); remaining=[]
   available=cfg['max_parallel_jobs']-len(live)
   for row in pending:
    if STOP: remaining.append(row); continue
    if expired_before_dispatch(row,cfg,server_now):
     directory=jobdir(row['id']); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
     meta=reply_error(124,'expired before dispatch: the request waited longer than its allowed queue age and was NOT executed',error_code='EXPIRED_BEFORE_DISPATCH',partial=False)
     write(directory/'result.json',meta); db.finish_job(row['id'],meta,executed=False); activity=True; continue
    if available<=0: remaining.append(row); continue
    try: plan=lock_plan(row,cfg,workspace(row,cfg)[2])
    except Exception: plan=[(GLOBAL_LOCK,True)]
    if conflicts(plan,live): remaining.append(row); continue
    if shutil.disk_usage(HOME).free<cfg.get('min_free_bytes',268435456): raise RuntimeError('low disk space; dispatch paused before side effects')
    if not db.claim(row['id'],socket.gethostname()): continue
    activity=True
    directory=jobdir(row['id']); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    try: request=start_job(row,cfg,plan=plan); live.append(request); available-=1
    except Exception as e:
     request=read(directory/'request.json')
     if active_unit(request): live.append(request); available-=1
     else:
      meta=reply_error(126,'job launch outcome failed: '+str(e)); write(directory/'result.json',meta); db.finish_job(row['id'],meta)
   publish_progress(db,live,progress_cache)
   if time.monotonic()-last_clean>300:
    try:cleanup(db);stats.pop('cleanup_error',None)
    except Exception as e:stats['cleanup_error']=type(e).__name__+': '+str(e)[:200]
    last_clean=time.monotonic()
   activity=activity or bool(live) or getattr(db,'delivered_in_pass',0)>0
   state.pop('last_error',None); state.pop('last_error_at',None)
   update_stats(stats,cfg,db,live,remaining,server_now)
   if time.monotonic()-last_beat>=1.0: db.heartbeat(state); last_beat=time.monotonic()
   streak=0
  except Exception as e:
   streak+=1; state['last_error']=type(e).__name__+': '+str(e)[:500]; state['last_error_at']=now(); core.log(state['last_error'])
   try: db.heartbeat(state); last_beat=time.monotonic()
   except Exception: pass
  try:write(HOME/'state.json',state)
  except OSError as e:core.log('state persistence failed: '+type(e).__name__)
  delay=next_delay(cfg,activity,streak)
  until=time.monotonic()+delay
  while not STOP and time.monotonic()<until: time.sleep(min(0.2,max(0.01,until-time.monotonic())))
 core.sd_notify('STOPPING=1')
 # Deliberately do not stop job units. They persist through coordinator restart.

if __name__=='__main__':
 if len(sys.argv)==3 and sys.argv[1]=='--worker': worker(sys.argv[2])
 else: daemon()
