#!/usr/bin/env python3
"""Bridge 3.0: reconnectable local jobs, safe retries, no Blocked state."""
import os,sys,json,time,signal,hashlib,base64,subprocess,fcntl,socket,shutil,uuid,threading,tempfile,traceback
from pathlib import Path
from datetime import datetime,timezone
from urllib.parse import quote
import bridge_core as core
import workspace_ops as ops
VERSION='supabase-bridge 3.0'
HOME=Path(os.environ.get('SF_BRIDGE_HOME',str(Path.home()/'supabase-bridge'))).resolve()
JOBS=HOME/'jobs'; CONFIG=HOME/'config.json'; STOP=False
CONTROLS={'job.status','job.output','job.cancel','bridge.health','workspace.list'}
core.BRIDGE_DIR=HOME; core.CONFIG_PATH=CONFIG; core.ENV_PATH=HOME/'.env'; core.STATE_PATH=HOME/'state.json'; core.LOG_PATH=HOME/'bridge.log'; core.VERSION=VERSION

def now(): return datetime.now(timezone.utc).isoformat()
def write(path,body): core.atomic_write(Path(path),json.dumps(body,ensure_ascii=True).encode())
def read(path,default=None):
 try: return json.loads(Path(path).read_text())
 except (OSError,ValueError): return default

def cfg_load():
 cfg=read(CONFIG)
 if not cfg: raise ValueError('missing bridge configuration')
 cfg['workspaces']=dict(cfg['workspaces']); cfg['workspaces'].update(read(HOME/'workspaces.json',{}))
 if cfg['default_workspace'] not in cfg['workspaces']: raise ValueError('default workspace missing')
 if not 1<=cfg.get('max_parallel_jobs',2)<=8: raise ValueError('max_parallel_jobs must be 1-8')
 return cfg

def workspace(row,cfg):
 p=row.get('payload') or {}; name=p.get('workspace',cfg['default_workspace'])
 if name not in cfg['workspaces']: raise ValueError('unknown workspace '+str(name))
 root=Path(cfg['workspaces'][name]).expanduser().resolve()
 cwd=Path(p.get('cwd','.' )).expanduser()
 cwd=(root/cwd).resolve() if not cwd.is_absolute() else cwd.resolve()
 if not cwd.is_dir(): raise ValueError('working directory does not exist')
 # Use root lock even for cwd override. Also use a global read/write lock for
 # full-trust shell and absolute paths, so bridge-managed writes never overlap.
 return name,str(root),str(cwd)

def jobdir(cid):
 cid=str(uuid.UUID(str(cid))); return JOBS/cid

def unit_name(cid,attempt): return 'sf-local-job-'+str(uuid.UUID(cid))+'-'+str(attempt)

def unit_state(unit):
 p=subprocess.run(['systemctl','--user','show',unit,'--property=ActiveState','--value'],capture_output=True,text=True,timeout=5)
 return p.stdout.strip() if p.returncode==0 else 'missing'

def active_unit(request):
 return request and unit_state(request['unit']) in ('active','activating','deactivating','reloading')

def reply_error(code,message):
 return {'exit_code':code,'result':json.dumps({'error':message}),'duration_ms':0,'finished_at':now(),'total_bytes':0}

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
 def lease():
  while not lease_stop.is_set():
   try: write(directory/'lease.json',{'pid':os.getpid(),'updated_at':now(),'phase':'running','attempt':request['attempt']})
   except OSError: pass
   lease_stop.wait(5)
 thread=threading.Thread(target=lease,daemon=True); thread.start()
 try:
  # Read-only workers share; shell and mutations are globally exclusive.
  lockdir=HOME/'locks'; lockdir.mkdir(parents=True,exist_ok=True,mode=0o700)
  lockpath=lockdir/'workspace-access.lock'; lock=open(lockpath,'a'); locks.append(lock)
  mode=fcntl.LOCK_SH if kind in ops.SAFE_KINDS else fcntl.LOCK_EX
  while True:
   if cancel.exists() or core.STOP_REQUESTED: raise InterruptedError('cancelled before execution')
   if time.monotonic()-started>=timeout: raise TimeoutError('runtime expired while waiting for workspace access')
   try: fcntl.flock(lock.fileno(),mode|fcntl.LOCK_NB); break
   except BlockingIOError: time.sleep(0.1)
  remaining=max(0.01,timeout-(time.monotonic()-started))
  write(directory/'started.json',{'started_at':now(),'pid':os.getpid(),'attempt':request['attempt'],'cwd':request['cwd']})
  if kind=='shell':
   execution_cfg={'workdir':request['cwd'],'output_max_chars':cfg['output_max_chars']}
   meta,head,tail=core.execute(execution_cfg,cid,row['command'],remaining,lambda:None)
   meta['result']=core.inline_result(meta,head,tail)
  else:
   body=ops.perform(kind,payload,request['cwd'],HOME,cfg)
   text=json.dumps(body,ensure_ascii=True)
   if len(text.encode())>32_000_000: raise ValueError('result exceeds 32 MB; use smaller pages')
   meta={'exit_code':0,'result':text,'total_bytes':len(text.encode()),'duration_ms':int((time.monotonic()-started)*1000),'finished_at':now()}
  if cancel.exists() or core.STOP_REQUESTED: meta=reply_error(130,'job cancelled; partial effects are possible')
 except InterruptedError as e: meta=reply_error(130,str(e))
 except TimeoutError as e: meta=reply_error(124,str(e))
 except Exception as e: meta=reply_error(1,type(e).__name__+': '+str(e)[:4000])
 finally:
  if meta is not None:
   meta['duration_ms']=int((time.monotonic()-started)*1000); meta['attempt']=request['attempt']
   write(directory/'result.json',meta)
  for lock in locks: lock.close()
  lease_stop.set(); thread.join(timeout=1)

def start_job(row,cfg,attempt=1):
 cid=row['id']; directory=jobdir(cid); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
 name,root,cwd=workspace(row,cfg)
 request={'row':row,'config':cfg,'workspace':name,'root':root,'cwd':cwd,'attempt':attempt,'unit':unit_name(cid,attempt),'submitted_at':now(),'release':str(Path(__file__).parent)}
 # Persist launch intent first. An absent unit after uncertain launch is never
 # used as permission to rerun writes.
 write(directory/'request.json',request)
 deadline=(row.get('timeout_sec') or cfg['exec_timeout_sec'])+15
 args=['systemd-run','--user','--collect','--unit='+request['unit'],'--property=Type=exec','--property=KillMode=control-group','--property=TimeoutStopSec=5','--property=RuntimeMaxSec='+str(deadline),'--property=MemoryMax='+cfg.get('job_memory_max','2G'),'--property=TasksMax='+str(cfg.get('job_tasks_max',512)),'--property=UMask=0077','--setenv=SF_BRIDGE_HOME='+str(HOME),sys.executable,str(Path(__file__).resolve()),'--worker',cid]
 p=subprocess.run(args,capture_output=True,text=True,timeout=10)
 if p.returncode: raise RuntimeError('job launch failed: '+p.stderr[:2000])
 return request

def recover_action(row,request,alive,meta,cancelled):
 if meta is not None: return 'deliver'
 if alive: return 'adopt'
 if cancelled: return 'cancel'
 attempt=request.get('attempt',1) if request else 0
 if row['kind'] in ops.SAFE_KINDS and attempt<3: return 'retry'
 return 'uncertain'

class DB(core.Supabase):
 def rows(self,filter): return self.rest('GET','/rest/v1/agent_commands?'+filter)[1]
 def running(self): return self.rows('status=eq.running&claimed_by=eq.'+quote(socket.gethostname(),safe='')+'&select=*')
 def pending(self,control=False):
  values=','.join(sorted(CONTROLS)); operator='in' if control else 'not.in'
  return self.rows('status=eq.pending&kind='+operator+'.('+values+')&order=created_at.asc,id.asc&limit=40&select=*')
 def finish_job(self,cid,meta):
  fields={'status':'done' if meta['exit_code']==0 else 'error','result':meta['result'],'exit_code':meta['exit_code'],'duration_ms':meta.get('duration_ms'),'executed':True,'finished_at':meta.get('finished_at',now())}
  ok=self.finish(cid,fields)
  if ok:
   first=not (jobdir(cid)/'delivered.json').exists()
   write(jobdir(cid)/'delivered.json',{'delivered_at':now()})
   if first and hasattr(self,'stats'):
    key='done' if meta['exit_code']==0 else 'error'; self.stats[key]=self.stats.get(key,0)+1
  return ok

def cancel_job(cid,db):
 directory=jobdir(cid); rows=db.rows('id=eq.'+cid+'&select=id,status')
 if not rows: raise ValueError('job not found')
 status=rows[0]['status']
 if status in ('done','error'): return {'id':cid,'status':status,'cancel_requested':False}
 directory.mkdir(parents=True,exist_ok=True,mode=0o700); write(directory/'cancel.json',{'requested_at':now()})
 request=read(directory/'request.json')
 if request:
  subprocess.run(['systemctl','--user','stop','--no-block',request['unit']],capture_output=True,text=True,timeout=5)
 else:
  meta=reply_error(130,'cancelled before job launch'); write(directory/'result.json',meta); db.finish_job(cid,meta)
 return {'id':cid,'cancel_requested':True}

def control(row,db,cfg):
 p=row.get('payload') or {}; kind=row['kind']
 if kind=='bridge.health': return {'version':VERSION,'free_bytes':shutil.disk_usage(HOME).free,'max_parallel_jobs':cfg['max_parallel_jobs'],'default_workspace':cfg['default_workspace'],'workspaces':cfg['workspaces']}
 if kind=='workspace.list': return ops.perform(kind,p,str(HOME),HOME,cfg)
 cid=str(uuid.UUID(str(p.get('id','')))); directory=jobdir(cid)
 if kind=='job.cancel': return cancel_job(cid,db)
 rows=db.rows('id=eq.'+cid+'&select=id,status,exit_code,duration_ms,started_at,finished_at')
 if not rows: raise ValueError('job not found')
 if kind=='job.status': return dict(rows[0],lease=read(directory/'lease.json'),attempt=(read(directory/'request.json') or {}).get('attempt'),output_bytes=(directory/(cid+'.log')).stat().st_size if (directory/(cid+'.log')).exists() else 0)
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
  if action=='deliver': db.finish_job(row['id'],meta)
  elif action=='adopt': live.append(request)
  elif action=='retry':
   try:
    if row['kind'] in CONTROLS:
     meta={'exit_code':0,'result':json.dumps(control(row,db,cfg)),'duration_ms':0,'finished_at':now()}; directory.mkdir(parents=True,exist_ok=True); write(directory/'result.json',meta); db.finish_job(row['id'],meta)
    else: live.append(start_job(row,cfg,(request or {}).get('attempt',0)+1))
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
 # Positively confirmed delivery receipts are the local cleanup prerequisite.
 count=0
 for directory in JOBS.iterdir():
  if count>=50: break
  if not directory.is_dir() or directory.is_symlink(): continue
  delivered=read(directory/'delivered.json')
  if not delivered or datetime.fromisoformat(delivered['delivered_at']).timestamp()>=cutoff: continue
  request=read(directory/'request.json')
  if active_unit(request): continue
  rows=db.rows('id=eq.'+directory.name+'&select=id,status')
  if not rows or rows[0]['status'] not in ('done','error'): continue
  shutil.rmtree(directory); db.rest('DELETE','/rest/v1/agent_commands?id=eq.'+rows[0]['id']+'&status=in.(done,error)',prefer='return=minimal'); count+=1
 # Clean legacy receipts only with positive DB confirmation, never let the
 # legacy row-only sweep delete durable v3 job references.
 locations=[HOME/'outputs',Path.home()/'.local/share/supabase-bridge/receipts',Path('/tmp/supabase-bridge-receipts')]
 grouped={}
 for location in locations:
  if not location.exists(): continue
  for item in location.iterdir():
   if item.is_symlink() or item.suffix not in ('.json','.log','.intent') or not item.is_file(): continue
   try: uuid.UUID(item.stem)
   except ValueError: continue
   grouped.setdefault(item.stem,[]).append(item)
 legacy=sorted(cid for cid,paths in grouped.items() if all(p.stat().st_mtime<cutoff for p in paths) and not jobdir(cid).exists())[:100]
 if legacy:
  rows=db.rows('id=in.('+','.join(legacy)+')&status=in.(done,error)&select=id,finished_at')
  for row in rows:
   if not row.get('finished_at') or datetime.fromisoformat(row['finished_at']).timestamp()>=cutoff: continue
   for artifact in grouped[row['id']]: artifact.unlink(missing_ok=True)
   db.rest('DELETE','/rest/v1/agent_commands?id=eq.'+row['id']+'&status=in.(done,error)',prefer='return=minimal')

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
 state=read(HOME/'state.json',{'stats':{'done':0,'error':0}}); db.stats=state.setdefault('stats',{}); streak=0; last_clean=0
 core.sd_notify('READY=1')
 while not STOP:
  core.sd_notify('WATCHDOG=1')
  try:
   cfg=cfg_load()
   # Control lane always progresses, even when normal job slots are full.
   for row in db.pending(True)[:8]:
    if not db.claim(row['id'],socket.gethostname()): continue
    directory=jobdir(row['id']); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    try: meta={'exit_code':0,'result':json.dumps(control(row,db,cfg)),'duration_ms':0,'finished_at':now()}
    except Exception as e: meta=reply_error(1,type(e).__name__+': '+str(e))
    write(directory/'result.json',meta); db.finish_job(row['id'],meta)
   live=reconcile(db,cfg)
   available=cfg['max_parallel_jobs']-len(live)
   if available>0:
    for row in db.pending(False):
     if available<=0 or STOP: break
     # Avoid occupying a slot with a job waiting behind an exclusive writer.
     live_write=any(r['row']['kind'] not in ops.SAFE_KINDS for r in live)
     if live_write or (live and row['kind'] not in ops.SAFE_KINDS): break
     if shutil.disk_usage(HOME).free<cfg.get('min_free_bytes',268435456): raise RuntimeError('low disk space; dispatch paused before side effects')
     if not db.claim(row['id'],socket.gethostname()): continue
     directory=jobdir(row['id']); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
     try: request=start_job(row,cfg); live.append(request); available-=1
     except Exception as e:
      request=read(directory/'request.json')
      if active_unit(request): live.append(request); available-=1
      else:
       meta=reply_error(126,'job launch outcome failed: '+str(e)); write(directory/'result.json',meta); db.finish_job(row['id'],meta)
   if time.monotonic()-last_clean>300: cleanup(db); last_clean=time.monotonic()
   state.pop('last_error',None); state.pop('last_error_at',None); state['stats']['active_jobs']=len(live)
   db.heartbeat(state); streak=0
  except Exception as e:
   streak+=1; state['last_error']=type(e).__name__+': '+str(e)[:500]; state['last_error_at']=now(); core.log(state['last_error'])
   try: db.heartbeat(state)
   except Exception: pass
  write(HOME/'state.json',state)
  delay=min(cfg.get('poll_interval_sec',3)*(2**min(streak,4)),60)
  until=time.monotonic()+delay
  while not STOP and time.monotonic()<until: time.sleep(0.2)
 core.sd_notify('STOPPING=1')
 # Deliberately do not stop job units. They persist through coordinator restart.

if __name__=='__main__':
 if len(sys.argv)==3 and sys.argv[1]=='--worker': worker(sys.argv[2])
 else: daemon()
