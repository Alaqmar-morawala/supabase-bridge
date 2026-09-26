import unittest,tempfile,os,sys,json,time,hashlib,subprocess,uuid,base64,threading
from pathlib import Path
from unittest.mock import patch
import agent_bridge as b
import workspace_ops as o
class OpsTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name); self.cfg={'default_workspace':'main','workspaces':{'main':str(self.root)},'exec_timeout_sec':20,'output_max_chars':1000000,'max_parallel_jobs':2}
 def tearDown(self): self.tmp.cleanup()
 def op(self,k,p): return o.perform(k,p,str(self.root),self.root,self.cfg)
 def test_atomic_create_edit_conflict(self):
  a=self.op('file.write',{'path':'a.txt','text':'alpha\nbeta\n'})
  self.assertEqual(self.op('file.hash',{'path':'a.txt'})['sha256'],a['sha256'])
  with self.assertRaises(ValueError): self.op('file.write',{'path':'a.txt','text':'bad'})
  c=self.op('file.edit',{'path':'a.txt','expected_sha256':a['sha256'],'edits':[{'old':'beta','new':'gamma'}]})
  self.assertEqual((self.root/'a.txt').read_text(),'alpha\ngamma\n')
  (self.root/'a.txt').write_text('outside change')
  with self.assertRaises(ValueError): self.op('file.edit',{'path':'a.txt','expected_sha256':c['sha256'],'edits':[{'old':'gamma','new':'bad'}]})
  self.assertEqual((self.root/'a.txt').read_text(),'outside change')
 def test_patch(self):
  a=self.op('file.write',{'path':'a','text':'a\nb\nc\n'})
  p='--- a\n+++ a\n@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n'
  self.op('file.patch',{'path':'a','expected_sha256':a['sha256'],'patch':p})
  self.assertEqual((self.root/'a').read_text(),'a\nB\nc\n')
  with self.assertRaises(ValueError): o.apply_unified('a\n',p)
 def test_line_and_byte_paging(self):
  self.op('file.write',{'path':'a','text':'one\ntwo\nthree\n'})
  r=self.op('file.read_lines',{'path':'a','start_line':2,'count':1})
  self.assertEqual(r['lines'],['two\n']); self.assertEqual(r['next_line'],3); self.assertTrue(r['has_more'])
  for length in [-1,0,4000001]:
   with self.assertRaises(ValueError): self.op('file.read',{'path':'a','length':length})
  os.mkfifo(self.root/'fifo')
  with self.assertRaises(ValueError): self.op('file.read',{'path':'fifo'})
 def test_large_rg_and_ignores(self):
  subprocess.run(['git','init','-q',str(self.root)],check=True)
  (self.root/'.gitignore').write_text('ignored/\n')
  (self.root/'ignored').mkdir(); (self.root/'ignored/secret.txt').write_text('needle')
  for i in range(1200): (self.root/f'file{i:04}.txt').write_text('needle '+str(i)+'\n')
  r=self.op('code.search',{'pattern':'needle','limit':20})
  self.assertEqual(len(r['matches']),20); self.assertTrue(r['truncated']); self.assertFalse(any('ignored' in m['path'] for m in r['matches']))
  g=self.op('code.glob',{'pattern':'file*.txt','limit':1000}); self.assertEqual(len(g['paths']),1000); self.assertTrue(g['truncated'])
 def test_git_and_worktree(self):
  subprocess.run(['git','init','-q',str(self.root)],check=True)
  (self.root/'a').write_text('a\n')
  subprocess.run(['git','-C',str(self.root),'add','a'],check=True)
  subprocess.run(['git','-C',str(self.root),'-c','user.name=Bridge Test','-c','user.email=bridge-test@localhost','commit','-qm','test'],check=True)
  self.assertIn('##',self.op('git.status',{})['text'])
  (self.root/'a').write_text('changed\n'); self.assertIn('changed',self.op('git.diff',{})['text'])
  result=self.op('workspace.create',{'name':'testtree','branch':'bridge-test-branch'})
  self.assertTrue(Path(result['path']).is_dir()); self.assertEqual(json.loads((self.root/'workspaces.json').read_text())['testtree'],result['path'])
 def test_retry_policy(self):
  for kind in ['shell','file.write','file.edit','file.patch','workspace.create','file.delete']:
   self.assertEqual(b.recover_action({'kind':kind},None,False,None,False),'uncertain')
  for kind in ['file.read','code.search','git.status']:
   self.assertEqual(b.recover_action({'kind':kind},{'attempt':1},False,None,False),'retry')
   self.assertEqual(b.recover_action({'kind':kind},{'attempt':3},False,None,False),'uncertain')
  self.assertEqual(b.recover_action({'kind':'shell'},{},True,None,False),'adopt')
  self.assertEqual(b.recover_action({'kind':'shell'},{},False,{'result':'ok'},False),'deliver')
  self.assertEqual(b.recover_action({'kind':'shell'},{},False,None,True),'cancel')
 def test_workspace_resolution(self):
  self.assertEqual(b.workspace({'payload':{}},self.cfg)[2],str(self.root))
  with self.assertRaises(ValueError): b.workspace({'payload':{'workspace':'missing'}},self.cfg)
class DurableTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.tmp=tempfile.TemporaryDirectory(); cls.home=Path(cls.tmp.name); cls.ws=cls.home/'workspace'; cls.ws.mkdir(); (cls.home/'jobs').mkdir()
  cls.cfg={'default_workspace':'main','workspaces':{'main':str(cls.ws)},'exec_timeout_sec':20,'output_max_chars':1000000,'max_parallel_jobs':2,'job_memory_max':'512M','job_tasks_max':64}
  cls.patch=patch.multiple(b,HOME=cls.home,JOBS=cls.home/'jobs'); cls.patch.start(); cls.units=[]
 @classmethod
 def tearDownClass(cls):
  for unit in cls.units: subprocess.run(['systemctl','--user','stop',unit],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
  cls.patch.stop(); cls.tmp.cleanup()
 def start(self,command,kind='shell',payload=None,timeout=20):
  cid=str(uuid.uuid4()); row={'id':cid,'kind':kind,'command':command,'payload':payload or {},'timeout_sec':timeout}
  req=b.start_job(row,self.cfg); self.units.append(req['unit']); return cid,req
 def wait(self,cid,seconds=25):
  until=time.monotonic()+seconds
  while time.monotonic()<until:
   meta=b.read(b.jobdir(cid)/'result.json')
   if meta is not None:return meta
   time.sleep(.1)
  self.fail('no receipt for '+cid)
 def test_01_job_survives_submitter_exit(self):
  # Launch from a short-lived subprocess, then adopt from this independent one.
  cid=str(uuid.uuid4()); row={'id':cid,'kind':'shell','command':'printf START; sleep 1; printf END','payload':{},'timeout_sec':10}
  code='import agent_bridge as b,json; from pathlib import Path; b.HOME=Path('+repr(str(self.home))+'); b.JOBS=b.HOME/"jobs"; print(json.dumps(b.start_job('+repr(row)+','+repr(self.cfg)+')))' 
  p=subprocess.run([sys.executable,'-c',code],capture_output=True,text=True,check=True,cwd=Path(b.__file__).parent)
  req=json.loads(p.stdout); self.units.append(req['unit']); self.assertTrue(b.active_unit(req))
  meta=self.wait(cid); self.assertEqual(meta['result'],'STARTEND'); self.assertEqual(meta['exit_code'],0)
 def test_02_file_job(self):
  cid,_=self.start('file.write','file.write',{'path':'jobfile','text':'job data'})
  self.assertEqual(self.wait(cid)['exit_code'],0)
  cid,_=self.start('file.read','file.read',{'path':'jobfile','text':True})
  self.assertEqual(json.loads(self.wait(cid)['result'])['text'],'job data')
 def test_03_output_cursor_and_cancel(self):
  cid,req=self.start('printf EARLY; sleep 10; printf LATE')
  deadline=time.monotonic()+5
  while time.monotonic()<deadline:
   path=b.jobdir(cid)/(cid+'.log')
   if path.exists() and path.stat().st_size>=5: break
   time.sleep(.1)
  class DB:
   def rows(s,*a):return [{'id':cid,'status':'running'}]
   def finish_job(s,*a):return True
  output=b.control({'kind':'job.output','payload':{'id':cid,'offset':0,'length':100}},DB(),self.cfg)
  self.assertIn('EARLY',output['text'])
  b.cancel_job(cid,DB()); meta=self.wait(cid); self.assertEqual(meta['exit_code'],130)
 def test_04_timeout(self):
  cid,_=self.start('sleep 5',timeout=1); self.assertEqual(self.wait(cid)['exit_code'],124)
 def test_05_write_lock_serializes(self):
  cid1,_=self.start('printf A >> locktest; sleep 1; printf B >> locktest')
  cid2,_=self.start('printf C >> locktest')
  self.wait(cid1); self.wait(cid2)
  text=(self.ws/'locktest').read_text(); self.assertIn(text,['ABC','CAB'])
 def test_06_pipe_rejected(self):
  os.mkfifo(self.ws/'pipe')
  cid,_=self.start('file.read','file.read',{'path':'pipe'},timeout=3)
  self.assertEqual(self.wait(cid)['exit_code'],1)
if __name__=='__main__': unittest.main(verbosity=2)
