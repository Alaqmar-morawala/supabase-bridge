import unittest,importlib.util,tempfile,os,json,time,sys,subprocess,uuid,base64
from pathlib import Path
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('repaired_bridge',Path(__file__).with_name('supabase_bridge.py'))
b=importlib.util.module_from_spec(spec); spec.loader.exec_module(b)
class BridgeTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(); self.base=Path(self.tmp.name); self.out=self.base/'outputs'; self.out.mkdir()
  self.cfg={'workdir':str(self.base),'output_max_chars':1000000,'exec_timeout_sec':20,'max_per_poll':10}
  self.patch=patch.multiple(b,OUTPUT_DIR=self.out,RECEIPT_DIRS=[self.out,self.base/'fallback'],STOP_REQUESTED=False,sd_notify=lambda *a:None,log=lambda *a:None)
  self.patch.start()
 def tearDown(self): self.patch.stop(); self.tmp.cleanup()
 def test_file_roundtrip_and_validation(self):
  op=b.run_file_op
  for length in [-1,0,4000001,True,'3']:
   self.assertEqual(op(self.cfg,'file.read',{'path':'missing','length':length})['exit_code'],1)
  self.assertEqual(op(self.cfg,'file.write',{'path':'nested/text','text':'a\r\n日本語'})['exit_code'],0)
  self.assertEqual(op(self.cfg,'file.write',{'path':'nested/text','text':'!','append':True})['exit_code'],0)
  value=json.loads(op(self.cfg,'file.read',{'path':'nested/text','text':True})['result'])
  self.assertEqual(value['text'],'a\r\n日本語!')
  self.assertFalse(value['has_more'])
  data=bytes(range(256))*30
  op(self.cfg,'file.write',{'path':'b','content_base64':base64.b64encode(data).decode()})
  page=json.loads(op(self.cfg,'file.read',{'path':'b','offset':100,'length':300})['result'])
  self.assertEqual(base64.b64decode(page['base64']),data[100:400]); self.assertTrue(page['has_more'])
  self.assertEqual(op(self.cfg,'file.write',{'path':'b','content_base64':'!!!!'})['exit_code'],1)
 def test_fifo_and_devices_rejected(self):
  os.mkfifo(self.base/'pipe')
  for path in [str(self.base/'pipe'),'/dev/zero']:
   start=time.monotonic(); meta=b.run_file_bounded(self.cfg,'file.read',{'path':path},3,lambda:None)
   self.assertEqual(meta['exit_code'],1); self.assertLess(time.monotonic()-start,3)
 def test_real_file_worker(self):
  meta=b.run_file_bounded(self.cfg,'file.write',{'path':'worker.txt','text':'worker'},3,lambda:None)
  self.assertEqual(meta['exit_code'],0)
  meta=b.run_file_bounded(self.cfg,'file.read',{'path':'worker.txt','text':True},3,lambda:None)
  self.assertEqual(json.loads(meta['result'])['text'],'worker')
 def test_worker_timeout_and_heartbeat(self):
  # Use the actual controller with a harmless sleeping worker stand-in.
  real=b.subprocess.Popen; progress=[]
  def sleeper(*a,**kw): return real([sys.executable,'-c','import time; time.sleep(10)'],**kw)
  with patch.object(b.subprocess,'Popen',side_effect=sleeper),patch.object(b,'HEARTBEAT_EVERY',0.1):
   meta=b.run_file_bounded(self.cfg,'file.read',{'path':'unused'},1.3,lambda:progress.append(True))
  self.assertEqual(meta['exit_code'],124); self.assertTrue(progress); self.assertLess(meta['duration_ms'],2500)
 def test_output_and_exact_cap(self):
  with patch.object(b,'DISK_CAP',100):
   meta,head,tail=b.execute(self.cfg,'cap',"python3 -c 'import os; os.write(1,b\"X\"*160)'",5,lambda:None)
  self.assertEqual((self.out/'cap.log').stat().st_size,100); self.assertEqual(meta['discarded_bytes'],60)
  self.assertIn('output loss',b.inline_result(meta,head,tail))
  meta,head,tail=b.execute(self.cfg,'raw',"python3 -c 'import os; os.write(1,b\"a\\r\\n\\xff\\xfe\")'",5,lambda:None)
  self.assertEqual((self.out/'raw.log').read_bytes(),b'a\r\n\xff\xfe')
 def test_closed_output_and_timeout(self):
  meta,_,_=b.execute(self.cfg,'closed','exec 1>&- 2>&-; sleep 1.1',3,lambda:None)
  self.assertEqual(meta['exit_code'],0); self.assertGreaterEqual(meta['duration_ms'],1000)
  meta,_,_=b.execute(self.cfg,'timeout','exec 1>&- 2>&-; sleep 3',0.3,lambda:None)
  self.assertEqual(meta['exit_code'],124)
 def test_tail(self):
  meta,head,tail=b.execute(self.cfg,'large',"python3 -c 'import os; os.write(1,b\"0123456789ABCDEF\"*80000)'",5,lambda:None)
  self.assertEqual(len(tail),50000); self.assertEqual(tail,(b'0123456789ABCDEF'*80000)[-50000:])
  text=b.inline_result(meta,head,tail); self.assertIn('1180000 bytes omitted',text)
 def test_cleanup_partial_and_fallback(self):
  fallback=self.base/'fallback'; fallback.mkdir()
  ids=[str(uuid.uuid4()) for _ in range(102)]; old=time.time()-8*86400
  for i,cid in enumerate(ids):
   p=(fallback if i%2 else self.out)/(cid+'.json'); p.write_text('{}'); os.utime(p,(old,old))
  blocked=ids[0]; missing=ids[1]; calls=[]
  class DB:
   def rest(s,method,path,*args,**kwargs):
    calls.append((method,path))
    if method=='GET' and 'id=in.(' in path:
     chunk=path.split('id=in.(')[1].split(')')[0].split(',')
     return 200,[{'id':cid,'status':'blocked' if cid==blocked else 'done','finished_at':datetime.fromtimestamp(old,timezone.utc).isoformat()} for cid in chunk if cid!=missing]
    return (200,[]) if method=='GET' else (204,[])
  b.housekeep(DB())
  remaining={p.stem for d in [self.out,fallback] for p in d.iterdir()}
  self.assertEqual(remaining,{blocked,missing})
  self.assertTrue(all(len(path.split('id=in.(')[1].split(')')[0].split(','))<=100 for method,path in calls if 'id=in.(' in path))
 def test_recovery_never_reruns(self):
  for cid,receipt,flag in [('a',False,False),('b',True,False),('c',False,True)]:
   row={'id':cid,'status':'running','executed':flag,'kind':'shell','command':'SHOULD_NOT_RUN'}
   if receipt: b.save_meta(cid,{'exit_code':0,'duration_ms':1,'result':'saved','total_bytes':5})
   calls=[]
   class DB:
    def fetch_undelivered(s,*a): return [dict(row)]
    def fetch_pending(s,*a): return []
    def requeue_stale(s,*a): return 0
    def reap_orphans(s,*a): return 0
    def finish(s,cid,fields): row.update(fields); return True
    def heartbeat(s,*a): pass
    def rest(s,method,*a,**kw): return (200,[]) if method=='GET' else (204,[])
   with patch.object(b,'load_creds',return_value=('','','')),patch.object(b,'Supabase',return_value=DB()),patch.object(b,'load_state',return_value={'stats':{}}),patch.object(b,'save_state'),patch.object(b,'execute',side_effect=AssertionError('rerun')):
    b.process_once(self.cfg,'runner')
   self.assertEqual(row['status'],'done' if receipt else 'blocked')
 def test_pending_intent_prevents_rerun(self):
  b.persist_receipt('id',{'runner':'runner'},'.intent')
  row={'id':'id','status':'pending','kind':'shell','command':'DO_NOT_RUN','executed':False}
  class DB:
   def fetch_undelivered(s,*a): return []
   def fetch_pending(s,*a): return [dict(row)]
   def requeue_stale(s,*a): return 0
   def reap_orphans(s,*a): return 0
   def finish(s,cid,fields): row.update(fields); return True
   def heartbeat(s,*a): pass
   def rest(s,method,*a,**kw): return (200,[]) if method=='GET' else (204,[])
  with patch.object(b,'load_creds',return_value=('','','')),patch.object(b,'Supabase',return_value=DB()),patch.object(b,'load_state',return_value={'stats':{}}),patch.object(b,'save_state'),patch.object(b,'execute',side_effect=AssertionError('rerun')):
   b.process_once(self.cfg,'runner')
  self.assertEqual(row['status'],'blocked')
 def test_empty_patch_response_not_success(self):
  sb=b.Supabase({},'https://unused','unused')
  with patch.object(sb,'rest',return_value=(200,[])),patch.object(b.time,'sleep'):
   self.assertFalse(sb.mark_executed('x')); self.assertFalse(sb.finish('x',{}))
 def test_receipt_failure_stops_before_execution(self):
  row={'id':'x','status':'pending','kind':'shell','command':'DO_NOT_RUN','executed':False}
  class DB:
   def fetch_undelivered(s,*a): return []
   def fetch_pending(s,*a): return [dict(row)]
   def requeue_stale(s,*a): return 0
   def reap_orphans(s,*a): return 0
   def claim(s,*a): row['status']='running'; return True
   def finish(s,cid,fields): row.update(fields); return True
   def heartbeat(s,*a): pass
   def rest(s,method,*a,**kw): return (200,[]) if method=='GET' else (204,[])
  with patch.object(b,'load_creds',return_value=('','','')),patch.object(b,'Supabase',return_value=DB()),patch.object(b,'load_state',return_value={'stats':{}}),patch.object(b,'save_state'),patch.object(b,'persist_receipt',side_effect=RuntimeError('disk full')),patch.object(b,'execute',side_effect=AssertionError('executed')):
   b.process_once(self.cfg,'runner')
  self.assertEqual(row['status'],'blocked')
if __name__=='__main__': unittest.main(verbosity=2)
