"""Read-only local payments source. Run: python api.py"""
import argparse,json
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlsplit,parse_qs,urlencode
from threading import Lock
root=Path(__file__).resolve().parent/'data';failed=set();lock=Lock()
class API(BaseHTTPRequestHandler):
 def send(self,code,data,broken=False):
  body=b'{"items":[' if broken else json.dumps(data).encode()
  self.send_response(code);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)))
  if code==503:self.send_header('Retry-After','1')
  self.end_headers();self.wfile.write(body)
 def do_GET(self):
  u=urlsplit(self.path);q={k:v[-1] for k,v in parse_qs(u.query).items()}
  if u.path!='/payments':return self.send(404,{'error':'unknown resource'})
  batch=q.get('batch','01_full')
  if batch not in {p.name for p in root.iterdir() if p.is_dir()}:return self.send(400,{'error':'unknown batch'})
  try:page=int(q.get('page',1));size=int(q.get('limit',10));assert page>=1 and 1<=size<=50
  except (AssertionError,ValueError):return self.send(400,{'error':'invalid page or limit'})
  fault=q.get('fault','');key=(batch,q.get('trial','default'),page)
  if page==2 and fault=='once':
   with lock:
    if key not in failed:failed.add(key);return self.send(503,{'error':'temporary failure'})
  if page==2 and fault=='always':return self.send(503,{'error':'unavailable'})
  if fault=='broken_json':return self.send(200,None,True)
  rows=json.loads((root/batch/'api_source.json').read_text(encoding='utf8'));end=page*size
  nxt='/payments?'+urlencode({**q,'page':page+1}) if end<len(rows) else None
  return self.send(200,{'items':rows[end-size:end],'total':len(rows),'next':nxt})
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--port',type=int,default=8765);a=p.parse_args()
 print('Payments API: http://127.0.0.1:%d/payments'%a.port,flush=True);ThreadingHTTPServer(('127.0.0.1',a.port),API).serve_forever()
