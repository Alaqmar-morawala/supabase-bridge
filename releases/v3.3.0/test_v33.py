import unittest,tempfile,json,subprocess
from pathlib import Path
from unittest.mock import patch,Mock
import agent_bridge as b
import workspace_ops as o
import bridge_lib as lib

def git(*args,cwd):
 return subprocess.run(['git','-c','user.email=t@example.com','-c','user.name=t','-c','commit.gpgsign=false',*args],cwd=cwd,capture_output=True,text=True,check=True).stdout

class Release33Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.home=Path(self.tmp.name);(self.home/'jobs').mkdir();(self.home/'workspaces').mkdir()
  self.repo=self.home/'repo';self.repo.mkdir()
  git('init','-q','-b','main',cwd=self.repo);(self.repo/'a.txt').write_text('one');git('add','a.txt',cwd=self.repo);git('commit','-q','-m','first',cwd=self.repo)
  (self.repo/'a.txt').write_text('two');git('commit','-q','-am','second',cwd=self.repo)
  self.p=patch.multiple(b,HOME=self.home,JOBS=self.home/'jobs');self.p.start()
  self.cfg={'default_workspace':'repo','workspaces':{'repo':str(self.repo)},'exec_timeout_sec':20,'output_max_chars':1000000,'max_parallel_jobs':2,'job_memory_max':'32G','job_tasks_max':4096,'poll_interval_sec':3}
 def tearDown(self):self.p.stop();self.tmp.cleanup()

 def test_version(self):
  self.assertEqual(b.VERSION,'supabase-bridge 3.3');self.assertEqual(b.PROTOCOL,'3.2')

 def test_git_log_and_show(self):
  for kind in ('git.log','git.show'):self.assertIn(kind,o.SAFE_KINDS);self.assertIn(kind,o.BATCH_ITEM_KINDS)
  log=o.perform('git.log',{'limit':5},str(self.repo),self.home,self.cfg)
  self.assertEqual([c['subject'] for c in log['commits']],['second','first']);self.assertEqual(len(log['commits'][0]['commit']),40);self.assertFalse(log['truncated'])
  self.assertEqual(log['commits'][0]['author'],'t');self.assertTrue(log['commits'][0]['date'])
  one=o.perform('git.log',{'limit':1,'path':'a.txt'},str(self.repo),self.home,self.cfg);self.assertEqual(len(one['commits']),1)
  show=o.perform('git.show',{'ref':'HEAD'},str(self.repo),self.home,self.cfg);self.assertIn('second',show['text']);self.assertIn('+two',show['text']);self.assertFalse(show['truncated'])
  for bad in [{'ref':'--output=/tmp/x'},{'ref':''},{'ref':5},{'limit':0},{'limit':201},{'limit':'5'}]:
   with self.assertRaises(ValueError):o.perform('git.log',bad,str(self.repo),self.home,self.cfg)
  with self.assertRaises(ValueError):o.validate_payload('git.show',{'ref':'-x'})
  with self.assertRaises(ValueError):o.perform('git.show',{'ref':'does-not-exist'},str(self.repo),self.home,self.cfg)
  batch=o.perform('batch',{'items':[{'kind':'git.log','payload':{'limit':1}},{'kind':'git.show','payload':{'ref':'HEAD'}}]},str(self.repo),self.home,self.cfg)
  self.assertEqual(batch['failed'],0);self.assertEqual(batch['items'][0]['data']['commits'][0]['subject'],'second')

 def test_workspace_remove(self):
  created=o.perform('workspace.create',{'name':'wt1','branch':'feature/wt1','ref':'HEAD'},str(self.repo),self.home,self.cfg)
  self.assertTrue(Path(created['path']).is_dir());self.assertIn('wt1',json.loads((self.home/'workspaces.json').read_text()))
  cfg=dict(self.cfg,workspaces=dict(self.cfg['workspaces'],wt1=created['path']))
  self.assertEqual(b.lock_plan({'kind':'workspace.remove','payload':{'name':'wt1'}},cfg,str(self.repo)),[(b.GLOBAL_LOCK,True)])
  (Path(created['path'])/'dirty.txt').write_text('x')
  with self.assertRaises(ValueError):o.perform('workspace.remove',{'name':'wt1'},str(self.repo),self.home,cfg)
  self.assertTrue(Path(created['path']).is_dir())
  removed=o.perform('workspace.remove',{'name':'wt1','force':True},str(self.repo),self.home,cfg)
  self.assertTrue(removed['removed']);self.assertFalse(Path(created['path']).exists());self.assertNotIn('wt1',json.loads((self.home/'workspaces.json').read_text()))
  self.assertIn('feature/wt1',git('branch','--list','feature/wt1',cwd=self.repo))
  for bad in [{'name':'repo'},{'name':'missing'},{'name':'../x'},{}]:
   with self.assertRaises(ValueError):o.perform('workspace.remove',bad,str(self.repo),self.home,cfg)
  with self.assertRaises(ValueError):o.validate_payload('workspace.remove',{'name':'wt1','force':'yes'})
  self.assertIn('workspace.remove',o.WRITE_KINDS);self.assertNotIn('workspace.remove',o.SAFE_KINDS)

 def test_publish_progress(self):
  cid='11111111-1111-4111-8111-111111111111';d=b.jobdir(cid);d.mkdir()
  b.write(d/'lease.json',{'phase':'executing'});(d/(cid+'.log')).write_bytes(b'x'*10)
  live=[{'row':{'id':cid,'kind':'shell'},'attempt':1,'submitted_at':b.now(),'locks':[['global',True]]}]
  db=Mock();db.rest.return_value=(200,[{'id':cid}]);cache={}
  b.publish_progress(db,live,cache)
  self.assertEqual(db.rest.call_count,1);method,path,body=db.rest.call_args[0]
  self.assertEqual(method,'PATCH');self.assertIn('status=eq.running',path);progress=json.loads(body['result'])['progress']
  self.assertEqual((progress['phase'],progress['output_bytes'],progress['attempt'],progress['protocol_version']),('executing',10,1,b.PROTOCOL))
  b.publish_progress(db,live,cache);self.assertEqual(db.rest.call_count,1)
  cache[cid]=(cache[cid][0],cache[cid][1]-6);(d/(cid+'.log')).write_bytes(b'x'*20)
  b.publish_progress(db,live,cache);self.assertEqual(db.rest.call_count,2)
  b.publish_progress(db,[],cache);self.assertEqual(cache,{})
  db.rest.side_effect=lib.BridgeError('down');b.publish_progress(db,live,cache);self.assertEqual(cache,{})

if __name__=='__main__':unittest.main()
