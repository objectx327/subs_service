"""Rebuild synthetic source deliveries; no third-party packages required."""
from pathlib import Path
from datetime import date,timedelta
import csv,json,copy
from collections import defaultdict
from decimal import Decimal as D
root=Path(__file__).resolve().parent
out=root/'data'
def dump(p,v):
 p.parent.mkdir(parents=True,exist_ok=True); p.write_text(json.dumps(v,ensure_ascii=False,indent=2)+'\n',encoding='utf8')
def money(v):return f'{D(v):.2f}'
def stamp(d,h=0):return d+f'T{h:02}:00:00Z'
keys={'customers':'customer_id','plans':'catalog_event_id','events':'event_id','invoices':'invoice_id','payments':'payment_id','refunds':'refund_id'}
log={k:[] for k in keys}
def add(k,when,**row):
 row.setdefault('version',1);row['updated_at']=when;log[k].append(row);return row
def amend(k,r,at,**kw):
 new={**r,**kw,'version':r['version']+1,'updated_at':at};log[k].append(new)
def ev(i,seq,d,status,plan):
 return add('events',stamp(d),event_id=f'E{i:04}-{seq}',subscription_id=f'S{i:04}',customer_id=f'C{((i-1)%80)+1:04}',effective_at=stamp(d),state=status,plan_id=plan)
prices={}
for tier,base in [('basic',12),('plus',24),('team',60)]:
 for cur,bump in [('EUR',0),('USD',3)]:
  plan=tier+'_'+cur;prices[plan]=(base+bump)
  for j,d in enumerate(['2025-12-01','2026-02-01']):
   add('plans',stamp(d),catalog_event_id=plan+'_'+d,plan_id=plan,effective_from=stamp(d),tier=tier,currency=cur,monthly_price=money(base+bump+j*2))
for i in range(1,81):
 c=add('customers',stamp('2025-12-01'),customer_id=f'C{i:04}',segment=['individual','business'][i%2],acquisition_channel=['app','web','partner'][i%3])
 if i%9==0:amend('customers',c,stamp('2026-02-12'),segment='business' if i%2==0 else 'individual')
for i in range(1,101):
 cur='USD' if i%4==0 else 'EUR'; plan=['basic','plus','team'][i%3]+'_'+cur
 start=0 if i<=80 else 1 if i<=90 else 2
 for m,d in enumerate(['2026-01-01','2026-02-01','2026-03-01']):
  if m<start:continue
  if m==start:ev(i,1,d,'active',plan)
  if m==1 and start==0 and i%6==0:
   plan=('team' if not plan.startswith('team') else 'plus')+'_'+cur;ev(i,2,d,'active',plan)
  if m==1 and i%10==0:
   ev(i,3,'2026-02-10','paused',plan);ev(i,4,'2026-02-19','active',plan)
  if m==2 and i%8==0:ev(i,5,'2026-03-15','cancelled',plan)
  inv=f'I{i:04}-{m+1:02}';val=D(prices[plan]+(2 if m else 0))
  add('invoices',stamp(d,9),invoice_id=inv,subscription_id=f'S{i:04}',issued_at=stamp(d,9),due_date=str(date.fromisoformat(d)+timedelta(days=9)),amount=money(val),currency=cur)
  for j,(frac,status) in enumerate([[(D('1'),'succeeded')],[(D('.5'),'succeeded')],[(D('.5'),'succeeded'),(D('.5'),'succeeded')],[(D('1'),'failed')],[]][i%5],1):
   day=d[:8]+('03' if j==1 else '12');at=stamp(day,12)
   if i==2 and m==0 and j==2:received=stamp('2026-02-06',8)
   else:received=at
   pa=add('payments',received,payment_id='P'+inv[1:]+f'-{j}',invoice_id=inv,paid_at=at,amount=money(val*frac),currency=cur,status=status)
   if i==1 and m==0 and j==1:amend('payments',pa,stamp('2026-03-02',10),amount=money(val*D('.75')))
   if i%15==0 and m==0 and j==1:add('refunds',stamp('2026-02-08',10),refund_id=f'R{i:04}',payment_id=pa['payment_id'],refunded_at=stamp('2026-02-08',10),amount=money(val/4),currency=cur,status='succeeded')
periods=[('01_full',None,'2026-02-01'),('02_delta','2026-02-01','2026-03-01'),('03_delta','2026-03-01','2026-04-01'),('04_full_reference',None,'2026-04-01'),('05_empty_delta','2026-04-01','2026-04-02')]
def materialize(k,lo,hi):
 rows=[r for r in log[k] if (not lo or r['updated_at']>=stamp(lo)) and r['updated_at']<stamp(hi)]
 if not lo:
  last={}
  for r in sorted(rows,key=lambda x:x['version']):last[r[keys[k]]]=r
  rows=list(last.values())
 return sorted(rows,key=lambda r:(r['updated_at'],r[keys[k]],r['version']))
def writecsv(p,rows,cols):
 with p.open('w',encoding='utf-8',newline='') as f:
  w=csv.DictWriter(f,fieldnames=cols);w.writeheader();w.writerows(rows)
def emit(name,rs,lo,hi,complete=True):
 folder=out/name;folder.mkdir(parents=True,exist_ok=True)
 for k in ['customers','plans','invoices','refunds']:writecsv(folder/(k+'.csv'),rs[k],list(log[k][0]))
 groups=defaultdict(list)
 for e in rs['events']:groups[(e['subscription_id'],e['customer_id'])].append({k:v for k,v in e.items() if k not in ['subscription_id','customer_id']})
 dump(folder/'subscriptions.json',{'subscriptions':[{'subscription_id':s,'customer_id':c,'events':events} for (s,c),events in groups.items()]})
 dump(folder/'api_source.json',rs['payments'])
 totals={}
 for k in ['invoices','payments','refunds']:
  t=defaultdict(D)
  for r in rs[k]:
   if r.get('status','succeeded')=='succeeded':t[r['currency']]+=D(r['amount'])
  totals[k]={c:money(v) for c,v in t.items()}
 dump(folder/'manifest.json',{'delivery':name,'mode':'delta' if lo else 'full','from_inclusive':stamp(lo) if lo else None,'until_exclusive':stamp(hi),'complete':complete,'counts':{k:len(v) for k,v in rs.items()},'amounts_by_currency':totals})
for name,lo,hi in periods:emit(name,{k:materialize(k,lo,hi) for k in keys},lo,hi)
for name in ['warning','critical','incomplete','stale_version','version_conflict']:
 lo,hi='2026-04-02','2026-04-03';rs={k:[] for k in keys}
 if name=='warning':rs['customers']=[{**log['customers'][0],'version':2,'updated_at':stamp(lo),'acquisition_channel':''}]
 if name=='critical':rs['payments']=[{**log['payments'][0],'payment_id':'P-TEST','invoice_id':'I-MISSING','amount':'-10.00','updated_at':stamp(lo)}]
 if name in ['stale_version','version_conflict']:
  rs['payments']=[{**log['payments'][0],'updated_at':stamp(lo)}]
  if name=='version_conflict':rs['payments'][0].update(version=2,amount='999.99')
 emit('test_'+name,rs,lo,hi,name!='incomplete')
