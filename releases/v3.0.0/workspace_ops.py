"""Workspace operations. No network or security-scanning operations are added."""
import os,json,hashlib,base64,stat,re,subprocess,select,time,signal,tempfile,fnmatch
from pathlib import Path
import bridge_core as core
SAFE_KINDS={'file.read','file.read_lines','file.list','file.stat','file.hash','code.search','code.glob','git.status','git.diff','workspace.list','bridge.health','job.status','job.output'}
WRITE_KINDS={'shell','file.write','file.edit','file.patch','file.delete','file.mkdir','workspace.create','job.cancel'}
KINDS=SAFE_KINDS|WRITE_KINDS
MAX_FILE=4_000_000

def integer(p,key,default,lo,hi):
 value=p.get(key,default)
 if isinstance(value,bool) or not isinstance(value,int) or not lo<=value<=hi: raise ValueError(f'{key} must be an integer in [{lo},{hi}]')
 return value

def file_path(p,cwd):
 value=p.get('path')
 if not isinstance(value,str) or not value: raise ValueError('path is required')
 path=Path(value).expanduser()
 return path if path.is_absolute() else Path(cwd)/path

def regular(path):
 fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK)
 if not stat.S_ISREG(os.fstat(fd).st_mode): os.close(fd); raise ValueError('regular file required')
 return os.fdopen(fd,'rb')

def digest(path):
 h=hashlib.sha256()
 with regular(path) as f:
  for block in iter(lambda:f.read(262144),b''): h.update(block)
 return h.hexdigest()

def atomic_edit(path,data,expected):
 path=Path(path)
 if path.is_symlink(): raise ValueError('edit the symlink target explicitly')
 exists=path.exists()
 current=digest(path) if exists else 'missing'
 if expected!=current: raise ValueError('file changed or expected_sha256 missing; read/hash the current file before editing')
 if len(data)>MAX_FILE: raise ValueError('atomic edit exceeds 4000000 bytes')
 path.parent.mkdir(parents=True,exist_ok=True)
 mode=stat.S_IMODE(path.stat().st_mode) if exists else 0o600
 fd,tmp=tempfile.mkstemp(prefix='.'+path.name+'.bridge-',dir=path.parent)
 try:
  with os.fdopen(fd,'wb') as f: f.write(data); f.flush(); os.fsync(f.fileno())
  os.chmod(tmp,mode)
  # Locks serialize bridge jobs. Recheck external changes just before replace.
  if (digest(path) if path.exists() else 'missing')!=current: raise ValueError('file changed during edit')
  os.replace(tmp,path)
  fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
  try: os.fsync(fd)
  finally: os.close(fd)
 finally:
  if os.path.exists(tmp): os.unlink(tmp)
 return {'path':str(path),'bytes':len(data),'previous_sha256':current,'sha256':hashlib.sha256(data).hexdigest()}

def apply_unified(original,patch):
 old=original.splitlines(keepends=True); diff=patch.splitlines(keepends=True)
 output=[]; cursor=0; i=0; hunks=0; headers=0
 while i<len(diff):
  line=diff[i]
  if line.startswith('--- '):
   headers+=1
   if headers>1: raise ValueError('one-file patches only')
   i+=1
   if i>=len(diff) or not diff[i].startswith('+++ '): raise ValueError('missing patch header')
   i+=1; continue
  if not line.startswith('@@ '):
   if line.startswith(('diff ','index ')) or not line.strip(): i+=1; continue
   raise ValueError('unsupported patch line')
  m=re.match(r'@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@',line)
  if not m: raise ValueError('invalid hunk')
  start=int(m.group(1)); oldcount=int(m.group(2) or 1); newcount=int(m.group(4) or 1)
  index=start-1 if oldcount else start
  if index<cursor or index>len(old): raise ValueError('hunk position mismatch')
  output.extend(old[cursor:index]); cursor=index; removed=added=0; i+=1
  while i<len(diff) and not diff[i].startswith('@@ '):
   item=diff[i]
   if item.startswith('--- '): break
   if item.startswith('\\ No newline'): raise ValueError('use file.edit for no-newline patches')
   if not item or item[0] not in ' +-': raise ValueError('invalid hunk body')
   prefix=item[0]; content=item[1:]
   if prefix in ' -':
    if cursor>=len(old) or old[cursor]!=content: raise ValueError('patch context mismatch')
    cursor+=1; removed+=1
   if prefix in ' +': output.append(content); added+=1
   i+=1
  if removed!=oldcount or added!=newcount: raise ValueError('hunk length mismatch')
  hunks+=1
 if not hunks: raise ValueError('no hunks')
 output.extend(old[cursor:]); return ''.join(output)

def run_bounded(args,cwd,limit=2_000_000,seconds=30):
 env=dict(os.environ,GIT_OPTIONAL_LOCKS='0'); env.pop('RIPGREP_CONFIG_PATH',None)
 proc=subprocess.Popen(args,cwd=cwd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,env=env)
 data=bytearray(); cutoff=False; start=time.monotonic()
 try:
  while True:
   if time.monotonic()-start>seconds: cutoff=True; break
   ready,_,_=select.select([proc.stdout],[],[],0.2)
   if ready:
    chunk=os.read(proc.stdout.fileno(),65536)
    if not chunk: break
    available=limit-len(data); data.extend(chunk[:available])
    if len(chunk)>available: cutoff=True; break
  if cutoff: proc.kill()
  code=proc.wait(timeout=5)
 finally:
  if proc.poll() is None: proc.kill(); proc.wait(timeout=5)
  proc.stdout.close()
 return code,bytes(data),cutoff

def git_args(): return ['git','--no-pager','-c','core.fsmonitor=false','-c','core.hooksPath=/dev/null']

def perform(kind,p,cwd,home,config):
 if not isinstance(p,dict): raise ValueError('payload must be an object')
 if kind=='file.hash':
  path=file_path(p,cwd); return {'path':str(path),'sha256':digest(path),'size':path.stat().st_size}
 if kind=='file.read_lines':
  path=file_path(p,cwd); start=integer(p,'start_line',1,1,2**31); count=integer(p,'count',200,1,2000)
  lines=[]; used=0; more=False
  with regular(path) as f:
   number=0
   while True:
    line=f.readline(1_000_001)
    if not line: break
    number+=1
    if len(line)>1_000_000: raise ValueError('line exceeds 1 MB; use byte-paged file.read')
    if number<start: continue
    if len(lines)>=count or used+len(line)>1_000_000: more=True; break
    lines.append(line.decode('utf-8','replace')); used+=len(line)
  return {'path':str(path),'start_line':start,'lines':lines,'next_line':start+len(lines),'has_more':more,'sha256':digest(path)}
 if kind in {'file.write','file.edit','file.patch'}:
  path=file_path(p,cwd)
  expected=p.get('expected_sha256','missing' if not path.exists() else None)
  if kind=='file.write':
   if 'content_base64' in p and 'text' in p: raise ValueError('choose text or base64')
   if 'content_base64' in p:
    encoded=p['content_base64']
    if not isinstance(encoded,str) or len(encoded)>5333336: raise ValueError('write too large')
    data=base64.b64decode(encoded,validate=True)
   else:
    if not isinstance(p.get('text',''),str): raise ValueError('text must be a string')
    data=p.get('text','').encode()
   if p.get('append') and path.exists():
    with regular(path) as f: previous=f.read(MAX_FILE+1)
    data=previous+data
  else:
   with regular(path) as f: data=f.read(MAX_FILE+1)
   if len(data)>MAX_FILE: raise ValueError('file too large for atomic edit')
   text=data.decode('utf-8')
   if kind=='file.patch': text=apply_unified(text,p.get('patch',''))
   else:
    edits=p.get('edits')
    if not isinstance(edits,list) or not 1<=len(edits)<=100: raise ValueError('edits must contain 1-100 replacements')
    for edit in edits:
     before=edit.get('old'); after=edit.get('new')
     if not isinstance(before,str) or not before or not isinstance(after,str): raise ValueError('old/new strings required')
     if text.count(before)!=1: raise ValueError('old text must occur exactly once')
     text=text.replace(before,after,1)
   data=text.encode()
  return atomic_edit(path,data,expected)
 if kind.startswith('file.'):
  meta=core.run_file_op({'workdir':cwd},kind,p)
  body=json.loads(meta['result'])
  if meta['exit_code']: raise ValueError(body.get('error','file operation failed'))
  if kind=='file.read': body['sha256']=digest(file_path(p,cwd)) if p.get('include_hash',False) else None
  return body
 if kind in {'code.search','code.glob'}:
  limit=integer(p,'limit',100,1,1000)
  base=['rg','--no-config']
  if p.get('hidden'): base+=['--hidden','--glob','!.git/**']
  if kind=='code.glob':
   pattern=p.get('pattern','*')
   if not isinstance(pattern,str) or len(pattern)>1000: raise ValueError('invalid pattern')
   args=base+['--files','--glob',pattern,'.']
  else:
   pattern=p.get('pattern')
   if not isinstance(pattern,str) or len(pattern)>10000: raise ValueError('search pattern required')
   args=base+['--json','--line-number','--max-columns','2000']
   if not p.get('regex',False): args+=['--fixed-strings']
   if p.get('ignore_case'): args+=['--ignore-case']
   if p.get('glob'): args+=['--glob',str(p['glob'])]
   args+=['--',pattern,'.']
  code,data,cut=run_bounded(args,cwd)
  if code not in (0,1) and not cut: raise ValueError(data.decode(errors='replace')[:1000])
  if kind=='code.glob':
   values=data.decode(errors='replace').splitlines(); return {'paths':values[:limit],'truncated':cut or len(values)>limit,'ignores_respected':True}
  matches=[]
  for line in data.splitlines():
   try: entry=json.loads(line)
   except ValueError: continue
   if entry.get('type')=='match':
    d=entry['data']; matches.append({'path':d['path'].get('text'),'line':d['line_number'],'text':d['lines'].get('text','')[:4000]})
  return {'matches':matches[:limit],'truncated':cut or len(matches)>limit,'ignores_respected':True}
 if kind in {'git.status','git.diff'}:
  if kind=='git.status': args=git_args()+['status','--porcelain=v1','--branch','--untracked-files=normal']
  else:
   args=git_args()+['diff','--no-ext-diff','--no-textconv']
   if p.get('staged'): args+=['--cached']
   if p.get('path'): args+=['--',str(p['path'])]
  code,data,cut=run_bounded(args,cwd)
  if code and not cut: raise ValueError(data.decode(errors='replace')[:2000])
  return {'text':data.decode(errors='replace'),'truncated':cut,'exit_code':code}
 if kind=='workspace.list':
  return {'default':config['default_workspace'],'workspaces':config['workspaces']}
 if kind=='workspace.create':
  name=p.get('name'); branch=p.get('branch'); ref=p.get('ref','HEAD')
  if not isinstance(name,str) or not re.fullmatch('[A-Za-z0-9_-]{1,60}',name): raise ValueError('workspace name must use letters, numbers, underscore or hyphen')
  if not isinstance(branch,str) or branch.startswith('-') or not isinstance(ref,str) or ref.startswith('-'): raise ValueError('branch and ref are required safe names')
  registry=Path(home)/'workspaces.json'; additions=json.loads(registry.read_text()) if registry.exists() else {}
  if name in config['workspaces'] or name in additions: raise ValueError('workspace name already exists')
  dest=Path(home)/'workspaces'/name
  if dest.exists(): raise ValueError('destination already exists')
  code,output,_=run_bounded(git_args()+['check-ref-format','--branch',branch],cwd)
  if code: raise ValueError('invalid branch name')
  code,output,cut=run_bounded(git_args()+['worktree','add','-b',branch,str(dest),ref],cwd,seconds=60)
  if code or cut: raise ValueError('worktree creation failed or timed out; inspect partial effects: '+output.decode(errors='replace')[:2000])
  additions[name]=str(dest); core.atomic_write(registry,json.dumps(additions).encode())
  return {'name':name,'path':str(dest),'branch':branch}
 raise ValueError('unsupported operation '+kind)
