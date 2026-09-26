import unittest,tempfile,json,uuid
from pathlib import Path
from unittest.mock import patch,Mock
from datetime import datetime,timezone,timedelta
import agent_bridge as b
import workspace_ops as o
import bridge_lib as lib

class ReleaseTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.home=Path(self.tmp.name);self.jobs=self.home/'jobs';self.jobs.mkdir()
  for name in ('wsA','wsB','outside'):(self.home/name).mkdir()
  self.p=patch.multiple(b,HOME=self.home,JOBS=self.jobs);self.p.start()
  self.cfg={'default_workspace':'A','workspaces':{'A':str(self.home/'wsA'),'B':str(self.home/'wsB')},'exec_timeout_sec':20,'output_max_chars':1000000,'max_parallel_jobs':2,'job_memory_max':'32G','job_tasks_max':4096,'poll_interval_sec':3,'active_poll_interval_sec':0.25,'max_queue_age_sec_safe':3600,'max_queue_age_sec_write':900}
 def tearDown(self):self.p.stop();self.tmp.cleanup()

 def test_library_has_no_daemon(self):
  src=Path(lib.__file__).read_text()
  for token in ('__main__','blocked','RECEIPT_DIRS','housekeep','block_uncertain','process_once'):self.assertNotIn(token,src)
  for name in ('main','loop','process_once','housekeep'):self.assertFalse(hasattr(lib,name),name)
  for name in ('BridgeError','Supabase','execute','atomic_write','run_file_op','load_creds'):self.assertTrue(hasattr(lib,name),name)
  self.assertIs(b.core,lib);self.assertIs(o.core,lib)

 def test_next_delay(self):
  self.assertEqual(b.next_delay(self.cfg,False,0),3.0)
  self.assertEqual(b.next_delay(self.cfg,True,0),0.25)
  self.assertEqual(b.next_delay(self.cfg,True,1),6)
  self.assertEqual(b.next_delay(self.cfg,False,4),48)
  self.assertEqual(b.next_delay(self.cfg,False,9),48)
  self.assertEqual(b.next_delay(dict(self.cfg,poll_interval_sec=5),False,9),60)

 def test_cfg_defaults_and_validation(self):
  base={'default_workspace':'A','workspaces':{'A':str(self.home/'wsA')},'exec_timeout_sec':20,'output_max_chars':1000000,'max_parallel_jobs':2,'job_memory_max':'32G','job_tasks_max':4096,'poll_interval_sec':3}
  config=self.home/'config.json'
  with patch.object(b,'CONFIG',config):
   b.write(config,base);cfg=b.cfg_load()
   self.assertEqual((cfg['active_poll_interval_sec'],cfg['max_queue_age_sec_safe'],cfg['max_queue_age_sec_write']),(0.25,3600,900))
   b.write(config,dict(base,active_poll_interval_sec=1));self.assertEqual(b.cfg_load()['active_poll_interval_sec'],1)
   for bad in [{'active_poll_interval_sec':0.01},{'active_poll_interval_sec':5},{'active_poll_interval_sec':'fast'},{'active_poll_interval_sec':True},{'max_queue_age_sec_write':0},{'max_queue_age_sec_safe':100000},{'max_queue_age_sec_safe':'1h'}]:
    b.write(config,dict(base,**bad))
    with self.assertRaises(ValueError):b.cfg_load()

 def test_lock_plan(self):
  cwdA=str(self.home/'wsA');cwdB=str(self.home/'wsB')
  self.assertEqual(b.lock_plan({'kind':'shell','payload':{}},self.cfg,cwdA),[(b.GLOBAL_LOCK,True)])
  self.assertEqual(b.lock_plan({'kind':'shell','payload':{'scope':'global'}},self.cfg,cwdA),[(b.GLOBAL_LOCK,True)])
  self.assertEqual(b.lock_plan({'kind':'shell','payload':{'scope':'workspace'}},self.cfg,cwdA),[(b.GLOBAL_LOCK,False),('ws:A',True)])
  self.assertEqual(b.lock_plan({'kind':'file.read','payload':{'path':'x.txt'}},self.cfg,cwdB),[(b.GLOBAL_LOCK,False),('ws:B',False)])
  self.assertEqual(b.lock_plan({'kind':'file.write','payload':{'path':'x.txt'}},self.cfg,cwdB),[(b.GLOBAL_LOCK,False),('ws:B',True)])
  self.assertEqual(b.lock_plan({'kind':'file.read','payload':{'path':str(self.home/'outside'/'x')}},self.cfg,cwdA),[(b.GLOBAL_LOCK,False)])
  self.assertEqual(b.lock_plan({'kind':'file.write','payload':{'path':str(self.home/'outside'/'x')}},self.cfg,cwdA),[(b.GLOBAL_LOCK,True)])
  self.assertEqual(b.lock_plan({'kind':'file.write','payload':{'path':'../outside/x'}},self.cfg,cwdA),[(b.GLOBAL_LOCK,True)])
  self.assertEqual(b.lock_plan({'kind':'code.search','payload':{'pattern':'x'}},self.cfg,cwdA),[(b.GLOBAL_LOCK,False),('ws:A',False)])
  self.assertEqual(b.lock_plan({'kind':'workspace.create','payload':{'name':'n','branch':'b'}},self.cfg,cwdA),[(b.GLOBAL_LOCK,True)])
  plan=b.lock_plan({'kind':'batch','payload':{'items':[{'kind':'file.read','payload':{'path':'a'}},{'kind':'git.status','payload':{'cwd':cwdB}}]}},self.cfg,cwdA)
  self.assertEqual(plan,[(b.GLOBAL_LOCK,False),('ws:A',False),('ws:B',False)])
  self.assertNotEqual(b.lock_path('ws:A'),b.lock_path('ws:B'));self.assertEqual(b.lock_path(b.GLOBAL_LOCK).name,'workspace-access.lock')

 def test_conflicts(self):
  shellA={'row':{'kind':'shell'},'locks':[['global',False],['ws:A',True]]}
  readB=[(b.GLOBAL_LOCK,False),('ws:B',False)];readA=[(b.GLOBAL_LOCK,False),('ws:A',False)];writeA=[(b.GLOBAL_LOCK,False),('ws:A',True)]
  self.assertFalse(b.conflicts(readB,[shellA]))
  self.assertTrue(b.conflicts(readA,[shellA]))
  self.assertTrue(b.conflicts(writeA,[shellA]))
  self.assertFalse(b.conflicts(readA,[{'row':{'kind':'file.read'},'locks':[['global',False],['ws:A',False]]}]))
  self.assertTrue(b.conflicts(readB,[{'row':{'kind':'shell'},'locks':[['global',True]]}]))
  self.assertTrue(b.conflicts(readB,[{'row':{'kind':'shell'}}]))
  self.assertFalse(b.conflicts(readB,[{'row':{'kind':'file.read'}}]))
  self.assertTrue(b.conflicts([(b.GLOBAL_LOCK,True)],[{'row':{'kind':'file.read'}}]))
  self.assertFalse(b.conflicts([(b.GLOBAL_LOCK,True)],[]))

 def test_dispatch_expiry(self):
  now=datetime.now(timezone.utc)
  def row(kind,age,**payload):return {'kind':kind,'payload':payload,'created_at':(now-timedelta(seconds=age)).isoformat()}
  self.assertTrue(b.expired_before_dispatch(row('shell',901),self.cfg,now))
  self.assertFalse(b.expired_before_dispatch(row('shell',899),self.cfg,now))
  self.assertTrue(b.expired_before_dispatch(row('file.write',901),self.cfg,now))
  self.assertFalse(b.expired_before_dispatch(row('file.read',1000),self.cfg,now))
  self.assertTrue(b.expired_before_dispatch(row('file.read',3601),self.cfg,now))
  self.assertTrue(b.expired_before_dispatch(row('file.read',101,max_queue_age_sec=100),self.cfg,now))
  self.assertEqual(b.queue_age_limit(row('shell',0,max_queue_age_sec=5000),self.cfg),900)
  self.assertEqual(b.queue_age_limit(row('shell',0,max_queue_age_sec=30),self.cfg),30)
  self.assertFalse(b.expired_before_dispatch({'kind':'shell','payload':{}},self.cfg,now))
  self.assertTrue(b.expired_before_dispatch({'kind':'shell','payload':{},'created_at':'2026-09-26T00:00:00Z'},self.cfg,now))
  meta=b.reply_error(124,'expired',error_code='EXPIRED_BEFORE_DISPATCH',partial=False);body=json.loads(meta['result'])
  self.assertEqual((meta['exit_code'],body['error_code'],body['partial_effects_possible'],body['protocol_version']),(124,'EXPIRED_BEFORE_DISPATCH',False,b.PROTOCOL))
  self.assertEqual(json.loads(b.reply_error(125,'x')['result'])['error_code'],'OUTCOME_UNCERTAIN')
  self.assertTrue(json.loads(b.reply_error(130,'x')['result'])['partial_effects_possible'])
  self.assertEqual(b.error_code_of(meta),'EXPIRED_BEFORE_DISPATCH');self.assertEqual(b.error_code_of({'exit_code':130,'result':'text'}),'CANCELLED');self.assertEqual(b.error_code_of({'exit_code':1,'result':'text'}),'OPERATION_FAILED')

 def test_batch_validation_and_perform(self):
  ws=self.home/'wsA';(ws/'a.txt').write_text('alpha');(ws/'b.txt').write_text('beta')
  bad_batches=[{'items':[]},{'items':[{'kind':'file.write','payload':{'path':'a.txt','text':'x','expected_sha256':'missing'}}]},{'items':[{'kind':'batch','payload':{'items':[]}}]},{'items':[{'kind':'file.read','payload':{'path':'a.txt','workspace':'A'}}]},{'items':[{'kind':'file.read','payload':{'path':'a.txt'}}]*51},{'items':[{'kind':'shell','payload':{}}]},{'items':[{'kind':'file.read','payload':{'path':'a.txt','text':'yes'}}]},{'items':[{'kind':'file.read'}]},{'items':'file.read'}]
  for bad in bad_batches:
   with self.assertRaises(ValueError):o.validate_payload('batch',bad)
  with self.assertRaises(ValueError):o.validate_payload('file.read',{'path':'a.txt','scope':'workspace'})
  with self.assertRaises(ValueError):o.validate_payload('shell',{'scope':'everything'})
  with self.assertRaises(ValueError):o.validate_payload('shell',{'max_queue_age_sec':0})
  o.validate_payload('shell',{'scope':'workspace','max_queue_age_sec':60})
  body=o.perform('batch',{'items':[{'kind':'file.read','payload':{'path':'a.txt','text':True,'include_hash':True}},{'kind':'file.hash','payload':{'path':'missing.txt'}},{'kind':'file.list','payload':{'path':'.'}}]},str(ws),self.home,self.cfg)
  self.assertEqual((body['count'],body['requested'],body['failed'],body['error_code']),(3,3,1,'BATCH_ITEM_FAILED'))
  self.assertEqual([item['status'] for item in body['items']],['ok','error','ok'])
  self.assertEqual(body['items'][0]['data']['text'],'alpha');self.assertEqual(body['items'][0]['data']['sha256'],o.digest(ws/'a.txt'))
  self.assertEqual(body['items'][1]['error_code'],'OPERATION_FAILED')
  self.assertEqual(sorted(entry['name'] for entry in body['items'][2]['data']['entries']),['a.txt','b.txt'])
  ok=o.perform('batch',{'items':[{'kind':'file.hash','payload':{'path':'a.txt'}},{'kind':'file.stat','payload':{'path':'b.txt','cwd':str(ws)}}]},str(ws),self.home,self.cfg)
  self.assertNotIn('error_code',ok);self.assertEqual((ok['failed'],ok['count']),(0,2))
  self.assertIn('batch',o.SAFE_KINDS)

 def test_stats_and_exclusive_job(self):
  db=Mock();db.count_pending.return_value=3
  started=datetime.now(timezone.utc)
  live=[{'row':{'id':'j1','kind':'shell','timeout_sec':100},'submitted_at':started.isoformat(),'locks':[['global',True]]}]
  now=started+timedelta(seconds=5)
  remaining=[{'created_at':(now-timedelta(seconds=42)).isoformat()},{'created_at':now.isoformat()}]
  stats=b.update_stats({},self.cfg,db,live,remaining,now)
  self.assertEqual((stats['queue_depth'],stats['oldest_pending_age_sec'],stats['active_jobs'],stats['version']),(3,42,1,b.VERSION))
  self.assertEqual(stats['current_exclusive_job']['id'],'j1');self.assertEqual(stats['current_exclusive_job']['deadline_at'],(started+timedelta(seconds=100)).isoformat());self.assertEqual(stats['current_exclusive_job']['locks'],['global'])
  self.assertEqual(stats['release']['version'],b.VERSION);self.assertIn('agent_bridge.py',stats['release']['files']);self.assertEqual(len(stats['release']['files']['bridge_lib.py']),64)
  self.assertIsNone(b.update_stats({},self.cfg,db,[{'row':{'kind':'file.read'}}],[],now)['current_exclusive_job'])
  self.assertIsNone(b.update_stats({},self.cfg,db,[],[],now)['oldest_pending_age_sec'])
  db.count_pending.side_effect=RuntimeError('offline');self.assertIsNone(b.update_stats({},self.cfg,db,[],[],now)['queue_depth'])
  health=b.control({'kind':'bridge.health','payload':{}},Mock(),self.cfg)
  self.assertEqual((health['version'],health['protocol_version'],health['stats'],health['active_poll_interval_sec']),(b.VERSION,b.PROTOCOL,{},0.25))
  self.assertEqual(health['release']['path'],str(b.RELEASE_DIR))

 def test_finish_job_counts_errors_and_keeps_expired_unexecuted(self):
  db=b.DB(self.cfg,'https://example.invalid','key',None);db.stats={'done':0,'error':0};captured=[]
  def finish(cid,fields):captured.append(fields);return True
  db.finish=finish
  cid=str(uuid.uuid4());b.jobdir(cid).mkdir()
  meta=b.reply_error(124,'expired',error_code='EXPIRED_BEFORE_DISPATCH',partial=False)
  self.assertTrue(db.finish_job(cid,meta,executed=False))
  self.assertFalse(captured[0]['executed']);self.assertEqual(captured[0]['status'],'error')
  self.assertEqual((db.stats['errors_by_code'],db.stats['error'],db.delivered_in_pass),({'EXPIRED_BEFORE_DISPATCH':1},1,1))
  self.assertTrue(db.finish_job(cid,meta,executed=False));self.assertEqual(db.stats['error'],1);self.assertEqual(db.delivered_in_pass,2)
  cid2=str(uuid.uuid4());b.jobdir(cid2).mkdir();self.assertTrue(db.finish_job(cid2,{'exit_code':0,'result':'ok','finished_at':b.now()}))
  self.assertTrue(captured[-1]['executed']);self.assertEqual(db.stats['done'],1);self.assertEqual(db.stats['last_finished_at'],captured[-1]['finished_at'])
  cid3=str(uuid.uuid4());b.jobdir(cid3).mkdir();self.assertTrue(db.finish_job(cid3,{'exit_code':130,'result':'plain cancel text','finished_at':b.now()}))
  self.assertEqual(db.stats['errors_by_code'],{'EXPIRED_BEFORE_DISPATCH':1,'CANCELLED':1})

 def test_count_pending_and_server_time(self):
  db=b.DB(self.cfg,'https://example.invalid','key',None);seen=[]
  def rest(method,path,body=None,prefer=None):
   seen.append(prefer);db.last_headers={'content-range':'0-0/7','date':'Sat, 26 Sep 2026 11:00:00 GMT'};return 200,[]
  db.rest=rest
  self.assertEqual(db.count_pending(),7);self.assertEqual(seen,['count=exact'])
  self.assertEqual(db.server_time(),datetime(2026,9,26,11,0,0,tzinfo=timezone.utc))
  def rest_unknown(method,path,body=None,prefer=None):
   db.last_headers={'content-range':'*/*'};return 200,[]
  db.rest=rest_unknown
  self.assertIsNone(db.count_pending())
  age=abs((db.server_time()-datetime.now(timezone.utc)).total_seconds());self.assertLess(age,5)

if __name__=='__main__':unittest.main()
