#!/usr/bin/env python3
"""PC quote API: licensed CSV catalogue + optional live Danawa data via ReefAPI.
No synthetic/default prices are generated. Provider credentials remain server-side.
"""
from __future__ import annotations
import argparse, csv, json, os, re, sqlite3, sys, threading, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen

BASE = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get('PC_CATALOG_DB', str(BASE / 'pc_catalog.sqlite3')))
REEF_API_KEY = os.environ.get('REEF_API_KEY', '').strip()
REEF_API_BASE = os.environ.get('REEF_API_BASE', 'https://api.reefapi.com').rstrip('/')
PRICE_CACHE_SECONDS = max(30, int(os.environ.get('PRICE_CACHE_SECONDS', '300')))
DB_LOCK = threading.RLock()
PRICE_CACHE = {}

CATEGORY_MAP = {
 'cpu':'cpu','processor':'cpu','프로세서':'cpu','중앙처리장치':'cpu',
 'motherboard':'motherboard','mainboard':'motherboard','메인보드':'motherboard',
 'ram':'ram','memory':'ram','메모리':'ram','gpu':'gpu','vga':'gpu','graphics':'gpu','그래픽카드':'gpu',
 'ssd':'ssd','hdd':'hdd','cooler':'cooler','cooling':'cooler','쿨러':'cooler','튜닝':'cooler',
 'case':'case','케이스':'case','psu':'psu','power':'psu','파워':'psu','software':'software','소프트웨어':'software','cable':'cable','케이블':'cable'
}

def now_iso(): return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
def valid_url(v):
 v=str(v or '').strip(); return v if v.startswith(('https://','http://')) else ''
def price_num(v):
 try: return int(float(str(v).replace(',','').replace('₩','').strip()))
 except (ValueError,TypeError): return 0

def connect():
 c=sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False); c.row_factory=sqlite3.Row
 c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); return c

def init_db():
 with DB_LOCK, connect() as db:
  db.execute('''CREATE TABLE IF NOT EXISTS products(id TEXT PRIMARY KEY, category TEXT NOT NULL, brand TEXT NOT NULL DEFAULT '', name TEXT NOT NULL, price_krw INTEGER NOT NULL DEFAULT 0, image_url TEXT NOT NULL DEFAULT '', product_url TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', socket TEXT NOT NULL DEFAULT '', memory_type TEXT NOT NULL DEFAULT '', form_factor TEXT NOT NULL DEFAULT '', capacity REAL NOT NULL DEFAULT 0, wattage REAL NOT NULL DEFAULT 0, tdp REAL NOT NULL DEFAULT 0, length_mm REAL NOT NULL DEFAULT 0, height_mm REAL NOT NULL DEFAULT 0, max_gpu_mm REAL NOT NULL DEFAULT 0, max_cooler_mm REAL NOT NULL DEFAULT 0, fan_size_mm REAL NOT NULL DEFAULT 0, cores REAL NOT NULL DEFAULT 0, threads REAL NOT NULL DEFAULT 0, slots_m2 REAL NOT NULL DEFAULT 0, specs_json TEXT NOT NULL DEFAULT '{}', updated_at TEXT NOT NULL)''')
  db.execute('CREATE INDEX IF NOT EXISTS idx_products_category_brand ON products(category,brand)')
  try:
   db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS product_search USING fts5(name,brand,description,content='products',content_rowid='rowid')")
   db.executescript('''CREATE TRIGGER IF NOT EXISTS products_ai AFTER INSERT ON products BEGIN INSERT INTO product_search(rowid,name,brand,description) VALUES(new.rowid,new.name,new.brand,new.description); END;
   CREATE TRIGGER IF NOT EXISTS products_ad AFTER DELETE ON products BEGIN INSERT INTO product_search(product_search,rowid,name,brand,description) VALUES('delete',old.rowid,old.name,old.brand,old.description); END;
   CREATE TRIGGER IF NOT EXISTS products_au AFTER UPDATE OF name,brand,description ON products BEGIN INSERT INTO product_search(product_search,rowid,name,brand,description) VALUES('delete',old.rowid,old.name,old.brand,old.description); INSERT INTO product_search(rowid,name,brand,description) VALUES(new.rowid,new.name,new.brand,new.description); END;''')
  except sqlite3.OperationalError: pass

def import_catalog_csv(path: Path):
 if not path.exists(): raise FileNotFoundError(f'CSV 파일을 찾을 수 없습니다: {path}')
 fields=['id','category','brand','name','price_krw','image_url','product_url','description','socket','memory_type','form_factor','capacity','wattage','tdp','length_mm','height_mm','max_gpu_mm','max_cooler_mm','fan_size_mm','cores','threads','slots_m2','specs_json','updated_at']
 sql=f"INSERT INTO products ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)}) ON CONFLICT(id) DO UPDATE SET "+','.join(f'{x}=excluded.{x}' for x in fields if x!='id')
 n=0
 with path.open('r',encoding='utf-8-sig',newline='') as f, DB_LOCK, connect() as db:
  reader=csv.DictReader(f); required={'id','category','brand','name','priceKrw','imageUrl','url'}; absent=required-set(reader.fieldnames or [])
  if absent: raise ValueError('CSV 필수 열이 없습니다: '+', '.join(sorted(absent)))
  for r in reader:
   pid=(r.get('id') or r.get('sku') or '').strip(); name=(r.get('name') or '').strip(); cat=CATEGORY_MAP.get((r.get('category') or '').strip().lower(),'')
   if not pid or not name or not cat: continue
   try: specs=json.dumps(json.loads(r.get('specsJson') or '{}'),ensure_ascii=False)
   except Exception: specs='{}'
   vals=[pid,cat,(r.get('brand') or '').strip(),name,price_num(r.get('priceKrw')),valid_url(r.get('imageUrl')),valid_url(r.get('url')),(r.get('description') or '').strip(),(r.get('socket') or '').strip(),(r.get('memory') or '').strip(),(r.get('formFactor') or '').strip(),price_num(r.get('capacity')),price_num(r.get('wattage')),price_num(r.get('tdp')),price_num(r.get('lengthMm')),price_num(r.get('heightMm')),price_num(r.get('maxGpuLengthMm')),price_num(r.get('maxCoolerHeightMm')),price_num(r.get('fanSizeMm')),price_num(r.get('cores')),price_num(r.get('threads')),price_num(r.get('slotsM2')),specs,now_iso()]
   db.execute(sql,vals); n+=1
  db.commit()
 print(f'CSV 가져오기 완료: {n:,}행 처리'); return n

def count_products():
 with DB_LOCK, connect() as db: return db.execute('SELECT COUNT(*) FROM products').fetchone()[0]

def reef_post(endpoint, payload):
 if not REEF_API_KEY: raise RuntimeError('REEF_API_KEY가 설정되지 않았습니다. 서버 환경변수에 API 키를 설정하세요.')
 req=Request(REEF_API_BASE+endpoint, data=json.dumps(payload).encode(), headers={'x-api-key':REEF_API_KEY,'Content-Type':'application/json','Accept':'application/json','User-Agent':'PCQuoteLiveCatalog/1.0'}, method='POST')
 with urlopen(req,timeout=25) as r: body=json.loads(r.read().decode('utf-8'))
 if isinstance(body,dict) and body.get('ok') is False: raise RuntimeError(str(body.get('error') or '가격 공급사 API 오류'))
 return body.get('data',body) if isinstance(body,dict) else body

def recursive_dicts(obj):
 if isinstance(obj,dict):
  yield obj
  for v in obj.values(): yield from recursive_dicts(v)
 elif isinstance(obj,list):
  for v in obj: yield from recursive_dicts(v)

def extract_products(data):
 # Provider fields may evolve; only normalize records that have a recognizable name and a real product URL/image.
 candidates=[]
 for x in recursive_dicts(data):
  name=str(x.get('name') or x.get('product_name') or x.get('title') or x.get('productName') or '').strip()
  pid=str(x.get('product_id') or x.get('productId') or x.get('pcode') or x.get('id') or '').strip()
  if not name or not pid: continue
  img=valid_url(x.get('image_url') or x.get('imageUrl') or x.get('image') or x.get('thumbnail') or x.get('img'))
  url=valid_url(x.get('url') or x.get('product_url') or x.get('productUrl'))
  if not url and pid.isdigit(): url='https://prod.danawa.com/info/?pcode='+pid
  if not img: continue
  price=price_num(x.get('lowest_price') or x.get('price') or x.get('lowestPrice') or x.get('price_krw'))
  brand=str(x.get('maker') or x.get('brand') or x.get('manufacturer') or '').strip()
  category='ssd' if re.search(r'\bssd\b|NVMe',name,re.I) else 'ram' if re.search(r'\bDDR[3454]\b|메모리|램',name,re.I) else 'cpu' if re.search(r'라이젠|코어 울트라|Core i[3579]|Xeon',name,re.I) else 'gpu' if re.search(r'RTX|Radeon|라데온|그래픽카드',name,re.I) else 'other'
  candidates.append({'id':'danawa:'+pid,'externalId':pid,'category':category,'brand':brand,'name':name,'priceKrw':price or None,'currency':'KRW','imageUrl':img,'url':url,'source':'다나와 가격비교 API','updatedAt':now_iso(),'priceStatus':'live' if price else 'not_available'})
 # deduplicate IDs preserving order
 seen=set(); out=[]
 for x in candidates:
  if x['id'] not in seen: seen.add(x['id']); out.append(x)
 return out

def live_search(q, category, page, page_size):
 # Provider API currently documents 40 rows/page and up to 50 pages; we expose requested page sizes to the app.
 data=reef_post('/danawa/v1/search',{'query':q or category or 'PC 부품','scope':'catalog','page':page})
 items=extract_products(data)
 if category: items=[x for x in items if x['category']==category or x['category']=='other']
 start=(page-1)*page_size; selected=items[start:start+page_size] if page_size<40 else items[:page_size]
 total=len(items)
 return {'items':selected,'total':total,'page':page,'pageSize':page_size,'currency':'KRW','updatedAt':now_iso(),'source':'Danawa via ReefAPI','live':True,'note':'가격은 공급사 응답 기준입니다. 이미지 URL가 제공된 상품만 표시합니다.'}

def live_product_price(pid):
 # Only Danawa IDs are resolved through the live product endpoint. Cache avoids repeated paid calls during rapid refreshes.
 key=str(pid).replace('danawa:','')
 cached=PRICE_CACHE.get(key)
 if cached and time.time()-cached[0] < PRICE_CACHE_SECONDS: return cached[1]
 data=reef_post('/danawa/v1/product',{'product_id':key})
 row=next((x for x in recursive_dicts(data) if any(k in x for k in ('lowest_price','lowestPrice','price'))),{})
 price=price_num(row.get('lowest_price') or row.get('lowestPrice') or row.get('price'))
 result={'id':key,'externalId':key,'priceKrw':price or None,'currency':'KRW','source':'다나와 가격비교 API','updatedAt':now_iso(),'priceStatus':'live' if price else 'not_available','url':'https://prod.danawa.com/info/?pcode='+key}
 PRICE_CACHE[key]=(time.time(),result); return result

class Handler(BaseHTTPRequestHandler):
 server_version='PCQuoteLiveCatalog/2.0'
 def log_message(self,fmt,*args): sys.stdout.write('[%s] %s\n'%(self.log_date_time_string(),fmt%args))
 def send_json(self,status,payload):
  b=json.dumps(payload,ensure_ascii=False).encode(); self.send_response(status); self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Content-Length',str(len(b))); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Cache-Control','no-store'); self.end_headers(); self.wfile.write(b)
 def do_OPTIONS(self):
  self.send_response(204); self.send_header('Access-Control-Allow-Origin','*'); self.send_header('Access-Control-Allow-Methods','GET, OPTIONS'); self.send_header('Access-Control-Allow-Headers','Content-Type'); self.end_headers()
 def do_GET(self):
  u=urlparse(self.path); p=parse_qs(u.query)
  try:
   if u.path=='/health': self.send_json(200,{'ok':True,'products':count_products(),'liveProviderConfigured':bool(REEF_API_KEY),'provider':'Danawa via ReefAPI' if REEF_API_KEY else None,'priceCacheSeconds':PRICE_CACHE_SECONDS,'updatedAt':now_iso()}); return
   if u.path=='/api/catalog/search':
    q=(p.get('q',[''])[0] or '').strip()[:160]; cat=CATEGORY_MAP.get((p.get('category',[''])[0] or '').strip().lower(),''); page=max(1,min(50,int(p.get('page',['1'])[0] or '1'))); size=max(1,min(40,int(p.get('pageSize',['24'])[0] or '24')))
    if REEF_API_KEY:
     if not q: q={'ram':'DDR5 RAM','ssd':'SSD','cpu':'CPU','gpu':'그래픽카드','motherboard':'메인보드'}.get(cat,'PC 부품')
     self.send_json(200,live_search(q,cat,page,size)); return
    self.send_json(503,{'error':'live_provider_not_configured','message':'실시간 상품 검색을 사용하려면 서버에 REEF_API_KEY를 설정해야 합니다. 임의 가격은 반환하지 않습니다.','items':[],'total':0}); return
   if u.path=='/api/parts/prices':
    ids=[x for x in (p.get('ids',[''])[0] or '').split(',') if x][:20]
    if REEF_API_KEY and ids:
     items=[]; errors=[]
     for pid in ids:
      try: items.append(live_product_price(pid))
      except Exception as e: errors.append({'id':pid,'error':str(e)})
     self.send_json(200,{'items':items,'updatedAt':now_iso(),'live':True,'errors':errors,'message':'공급사 API 응답에 가격이 있을 때만 가격을 표시합니다.'}); return
    self.send_json(503 if not REEF_API_KEY else 200,{'items':[{'id':'__connection_test__','priceKrw':None,'currency':'KRW'}] if REEF_API_KEY and not ids else [],'updatedAt':now_iso(),'live':False,'priceFeedConfigured':bool(REEF_API_KEY),'message':'실제 가격을 가져올 상품 ID 또는 공급사 API 키가 없습니다. 가격을 임의 생성하지 않았습니다.'}); return
           if u.path in ('/', '/pc_quote_app.html'):
            html_path = BASE / 'pc_quote_app.html'
            if html_path.exists():
                body = html_path.read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
   self.send_json(404,{'error':'not_found','message':'지원하지 않는 경로입니다.'})
  except Exception as e:
   self.send_json(502,{'error':'provider_error','message':str(e),'items':[],'live':False})

def main():
 global DB_PATH
 ap=argparse.ArgumentParser(); ap.add_argument('--import',dest='csv_path'); ap.add_argument('--import-only',action='store_true'); ap.add_argument('--db',default=str(DB_PATH)); ap.add_argument('--host',default=os.environ.get('HOST','0.0.0.0')); ap.add_argument('--port',type=int,default=int(os.environ.get('PORT','8787'))); a=ap.parse_args()
 DB_PATH=Path(a.db).resolve(); init_db()
 if a.csv_path: import_catalog_csv(Path(a.csv_path).resolve())
 if a.import_only: return
 print('PC 견적 API 서버 시작'); print(f'DB: {DB_PATH}'); print(f'상품 수(로컬 DB): {count_products():,}'); print(f'실시간 다나와 API: {"설정됨" if REEF_API_KEY else "미설정 - 임의 가격 미사용"}'); print(f'포트: {a.port}')
 ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
if __name__=='__main__': main()
