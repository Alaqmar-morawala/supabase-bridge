import unittest,tempfile,os,time,json,uuid,subprocess,hashlib,errno,sys
from pathlib import Path
from unittest.mock import patch,Mock
from datetime import datetime,timezone
import agent_bridge as b
import workspace_ops as o
class HardenedTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.home=Path(self.tmp.name);self.jobs=self.home/'jobs';self.jobs.mkdir()
  self.p=patch.multiple(b,HOME=self.home,JOBS=self.jobs);self.p.start()
  self.cfg={'default_workspace':'main','workspaces':{'main':str(self.home)},'exec_timeout_sec':20,'output_max_chars':1000000,'max_parallel_jobs':2,'job_memory_max':'32G','job_tasks_max':4096,'poll_interval_sec':3}
 def tearDown(self):self.p.stop();self.tmp.cleanup()
 def makejob(self):
  cid=str(uuid.uuid4());d=b.jobdir(cid);d.mkdir();return cid,d
 def test_systemd_query_failure_is_not_missing(self):
  with patch.object(b.subprocess,'run',side_effect=subprocess.TimeoutExpired('systemctl',5)):
   self.assertEqual(b.unit_state('sf-local-job-x'),'unknown');self.assertTrue(b.active_unit({'unit':'sf-local-job-x'}))
  with patch.object(b.subprocess,'run',return_value=Mock(returncode=1,stdout='',stderr='bus disconnected')):
   self.assertEqual(b.unit_state('sf-local-job-x'),'unknown')
  with patch.object(b.subprocess,'run',return_value=Mock(returncode=1,stdout='LoadState=not-found\nActiveState=inactive\n')):
   self.assertFalse(b.active_unit({'unit':'sf-local-job-x'}))
 def test_cancel_before_launch_prevents_process(self):
  cid,d=self.makejob();b.write(d/'cancel.json',{'requested_at':b.now()})
  with patch.object(b.subprocess,'run',side_effect=AssertionError('launched')):
   with self.assertRaises(InterruptedError):b.start_job({'id':cid,'kind':'shell','payload':{},'command':'true'},self.cfg)
 def test_cancel_after_receipt_preserves_success(self):
  cid,d=self.makejob();meta={'exit_code':0,'result':'already completed','finished_at':b.now()};b.write(d/'result.json',meta)
  db=Mock();db.rows.return_value=[{'status':'running'}];db.finish_job.return_value=True
  with patch.object(b.subprocess,'run',side_effect=AssertionError('signal')):result=b.cancel_job(cid,db)
  self.assertFalse(result['cancel_requested']);self.assertFalse((d/'cancel.json').exists());db.finish_job.assert_called_once_with(cid,meta)
 def test_unknown_cancel_retried_on_recovery(self):
  cid,d=self.makejob();req={'unit':'sf-local-job-x','row':{'kind':'shell'}};b.write(d/'request.json',req)
  db=Mock();db.rows.return_value=[{'status':'running'}]
  with patch.object(b,'unit_state',return_value='unknown'):
   r=b.cancel_job(cid,db);self.assertFalse(r['confirmed_stopped'])
  db.running.return_value=[{'id':cid,'kind':'shell'}]
  with patch.object(b,'unit_state',return_value='active'),patch.object(b.subprocess,'run',return_value=Mock(returncode=0)) as stop:
   self.assertEqual(len(b.reconcile(db,self.cfg)),1);self.assertIn('stop',stop.call_args.args[0])
 def test_finish_ack_loss_adopts_db_result(self):
  cid,d=self.makejob();meta={'exit_code':0,'result':'ok','duration_ms':3,'finished_at':b.now()}
  db=b.DB({},'https://unused','unused')
  with patch.object(db,'finish',return_value=False),patch.object(db,'rows',return_value=[{'status':'done','exit_code':0,'result':'ok'}]):
   self.assertTrue(db.finish_job(cid,meta));self.assertTrue((d/'delivered.json').exists())
 def test_completion_keeps_slot_until_worker_exits(self):
  self.assertEqual(b.recover_action({'kind':'shell'},{},True,{'exit_code':0},False),'adopt')
 def old_result(self,d):
  old=datetime.fromtimestamp(time.time()-8*86400,timezone.utc).isoformat();b.write(d/'result.json',{'finished_at':old,'exit_code':0,'result':'ok'});return old
 def test_cleanup_delete_failure_preserves_evidence(self):
  cid,d=self.makejob();old=self.old_result(d);db=Mock();db.rows.return_value=[{'id':cid,'status':'done','finished_at':old}];db.rest.side_effect=b.core.BridgeError('network down')
  with self.assertRaises(b.core.BridgeError):b.cleanup(db)
  self.assertTrue((d/'result.json').exists());self.assertTrue((d/'cleanup.json').exists())
 def test_cleanup_recovers_lost_delete_ack(self):
  cid,d=self.makejob();self.old_result(d);b.write(d/'cleanup.json',{'job_id':cid});db=Mock();db.rows.return_value=[]
  b.cleanup(db);self.assertFalse(d.exists())
 def test_cleanup_missing_delivery_marker(self):
  cid,d=self.makejob();old=self.old_result(d);db=Mock();db.rows.return_value=[{'id':cid,'status':'done','finished_at':old}];db.rest.return_value=(200,[{'id':cid}])
  b.cleanup(db);self.assertFalse(d.exists())
 def test_cleanup_absent_row_without_intent_retained(self):
  cid,d=self.makejob();self.old_result(d);db=Mock();db.rows.return_value=[]
  b.cleanup(db);self.assertTrue(d.exists());db.rest.assert_not_called()
 def test_paged_running_claims(self):
  db=b.DB({},'https://unused','unused');first=[{'id':str(i)} for i in range(100)]
  with patch.object(db,'rows',side_effect=[first,[{'id':'last'}]]) as rows:
   self.assertEqual(len(db.running()),101);self.assertIn('offset=100',rows.call_args.args[0])
 def test_hash_same_descriptor_and_content(self):
  p=self.home/'a';p.write_bytes(b'one\ntwo\nthree\n')
  r=o.perform('file.read',{'path':str(p),'text':True,'length':4,'include_hash':True},str(self.home),self.home,self.cfg)
  self.assertEqual(r['text'],'one\n');self.assertEqual(r['sha256'],hashlib.sha256(p.read_bytes()).hexdigest())
  r=o.perform('file.read_lines',{'path':str(p),'start_line':2,'count':1},str(self.home),self.home,self.cfg)
  self.assertEqual(r['lines'],['two\n']);self.assertEqual(r['sha256'],hashlib.sha256(p.read_bytes()).hexdigest())
 def test_changed_during_read_fails_not_mixed_hash(self):
  p=self.home/'a';p.write_text('before');real=o.check_snapshot
  def changed(path,f,before):p.write_text('after!');return real(path,f,before)
  with patch.object(o,'check_snapshot',side_effect=changed):
   with self.assertRaisesRegex(ValueError,'FILE_CHANGED'):o.perform('file.read',{'path':str(p),'include_hash':True},str(self.home),self.home,self.cfg)
 def test_stale_edit_input_and_strict_bools(self):
  p=self.home/'a';p.write_text('changed')
  with self.assertRaisesRegex(ValueError,'FILE_CHANGED'):o.perform('file.edit',{'path':str(p),'expected_sha256':'0'*64,'edits':[{'old':'changed','new':'bad'}]},str(self.home),self.home,self.cfg)
  with self.assertRaises(ValueError):o.perform('file.delete',{'path':str(p),'recursive':'false'},str(self.home),self.home,self.cfg)
  self.assertEqual(p.read_text(),'changed')
 def test_atomic_disk_failure_preserves_original(self):
  p=self.home/'a';p.write_bytes(b'original');expected=o.digest(p)
  with patch.object(o.os,'fsync',side_effect=OSError(errno.ENOSPC,'simulated full disk')):
   with self.assertRaises(OSError):o.atomic_edit(p,b'new',expected)
  self.assertEqual(p.read_bytes(),b'original');self.assertEqual(list(self.home.glob('.a.bridge-*')),[])
 def test_bad_configuration_rejected(self):
  for change in [{'job_memory_max':'infinity'},{'job_tasks_max':True},{'poll_interval_sec':0}]:
   config=dict(self.cfg,**change)
   with patch.object(b,'read',side_effect=[config,{}]):
    with self.assertRaises(ValueError):b.cfg_load()
 def test_finish_receipt_disk_failure_not_reexecution(self):
  cid,d=self.makejob();db=b.DB({},'https://unused','unused');meta={'exit_code':0,'result':'ok','finished_at':b.now()}
  with patch.object(db,'finish',return_value=True),patch.object(b,'write',side_effect=OSError(errno.ENOSPC,'full')):
   self.assertTrue(db.finish_job(cid,meta))

 def test_repeated_network_failure_does_not_repeat_write(self):
  cid,d=self.makejob();meta={'exit_code':0,'result':'effect completed','finished_at':b.now()};b.write(d/'result.json',meta)
  db=Mock();db.running.return_value=[{'id':cid,'kind':'shell'}];db.finish_job.side_effect=b.core.BridgeError('outage')
  with patch.object(b,'start_job',side_effect=AssertionError('reexecuted')):
   for _ in range(20):
    with self.assertRaises(b.core.BridgeError):b.reconcile(db,self.cfg)
   db.finish_job.side_effect=None;db.finish_job.return_value=True;b.reconcile(db,self.cfg)
  self.assertEqual(db.finish_job.call_count,21);self.assertEqual(b.read(d/'result.json')['result'],'effect completed')
 def test_retry_capacity_is_bounded(self):
  rows=[]
  for _ in range(4):
   cid,d=self.makejob();rows.append({'id':cid,'kind':'file.read','payload':{'path':'x'}})
  db=Mock();db.running.return_value=rows
  def start(row,cfg,attempt=1):return {'row':row,'attempt':attempt,'unit':'sf-local-job-test'}
  with patch.object(b,'start_job',side_effect=start) as launch:
   live=b.reconcile(db,self.cfg)
  self.assertEqual(len(live),2);self.assertEqual(launch.call_count,2)
 def test_control_waiting_normal_job_does_not_occupy_slot(self):
  # Control requests are selected separately. Fixed per-pass budget leaves
  # normal dispatch a chance each pass; reads cannot jump ahead of a writer.
  db=b.DB({},'https://unused','unused')
  with patch.object(db,'rows',return_value=[]) as rows:
   db.pending(True);self.assertIn('kind=in.',rows.call_args.args[0])
   db.pending(False);self.assertIn('kind=not.in.',rows.call_args.args[0])

if __name__=='__main__':unittest.main(verbosity=2)
