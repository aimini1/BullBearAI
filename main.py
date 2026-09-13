import os
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
import upstox_client
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title='BullBear AI - Live Market Radar')
INSTRUMENTS={'NIFTY 50':'NSE_INDEX|Nifty 50','BANK NIFTY':'NSE_INDEX|Nifty Bank','INDIA VIX':'NSE_INDEX|India VIX'}
state={n:{'instrument_key':k,'ltp':None,'change_pct':None,'last_update':None} for n,k in INSTRUMENTS.items()}
state['connection']='starting'; state['error']=None

def extract_ltp(feed):
    if not isinstance(feed,dict): return
    feeds=feed.get('feeds',{})
    for name,key in INSTRUMENTS.items():
        item=feeds.get(key)
        if not item: continue
        ltpc=item.get('ltpc')
        if not ltpc:
            ff=item.get('fullFeed',{}).get('marketFF',{})
            ltpc=ff.get('ltpc')
        if ltpc and ltpc.get('ltp') is not None:
            ltp=float(ltpc['ltp']); cp=ltpc.get('cp')
            change=None if cp in (None,0) else round((ltp-float(cp))/float(cp)*100,2)
            state[name].update(ltp=ltp,change_pct=change,last_update=datetime.now(ZoneInfo('Asia/Kolkata')).isoformat())

def update_bias():
    n = state["NIFTY 50"]["change_pct"]
    b = state["BANK NIFTY"]["change_pct"]
    v = state["INDIA VIX"]["change_pct"]
    if n is None or b is None or v is None:
        return
    score = 50.0
    score += max(-20, min(20, n * 8))
    score += max(-15, min(15, b * 6))
    # Rising VIX is treated as a risk-off input; falling VIX as risk-on.
    score += max(-10, min(10, -v * 3))
    score = max(0, min(100, score))
    if score >= 60:
        label = "BULLISH"
    elif score <= 40:
        label = "BEARISH"
    else:
        label = "NEUTRAL"
    state["bias"] = {
        "label": label,
        "confidence": round(abs(score - 50) * 2, 1),
        "score": round(score, 1),
        "reason": f"Nifty {n:+.2f}% | Bank Nifty {b:+.2f}% | VIX {v:+.2f}%"
    }

def start_streamer():
    token=os.getenv('UPSTOX_ACCESS_TOKEN')
    if not token: state['connection']='token_not_configured'; return
    try:
        cfg=upstox_client.Configuration(); cfg.access_token=token
        streamer=upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg))
        def on_open():
            state['connection']='connected'; state['error']=None
            streamer.subscribe(list(INSTRUMENTS.values()),'ltpc')
        def on_message(message):
            try: extract_ltp(message)
            except Exception as e: state['error']=f'feed_parse_error: {e}'
        def on_error(error): state['connection']='error'; state['error']=str(error)
        def on_close(*args): state['connection']='closed'
        streamer.on('open',on_open); streamer.on('message',on_message); streamer.on('error',on_error); streamer.on('close',on_close)
        state['connection']='connecting'; streamer.connect()
    except Exception as e: state['connection']='error'; state['error']=str(e)

@app.on_event('startup')
def startup(): threading.Thread(target=start_streamer,daemon=True).start()

@app.get('/api/live')
def live(): return {'connection':state['connection'],'error':state['error'],'instruments':{k:v for k,v in state.items() if k in INSTRUMENTS}}

@app.get('/',response_class=HTMLResponse)
def dashboard():
    return HTMLResponse('''<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>BullBear AI</title><style>body{margin:0;background:#f4f6fa;color:#17202a;font-family:Arial}.wrap{max-width:900px;margin:auto;padding:18px}header{display:flex;justify-content:space-between;align-items:center}h1{margin:0;font-size:23px}.muted{color:#687383;font-size:12px}.status{padding:8px 10px;border-radius:9px;background:#fff;border:1px solid #dfe5ed;font-size:12px}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:18px}.card{background:#fff;border:1px solid #dfe5ed;border-radius:15px;padding:16px}.k{font-size:11px;color:#687383;font-weight:bold}.v{font-size:26px;font-weight:800;margin:9px 0}.pos{color:#168451}.neg{color:#b83a32}.wait{color:#9a6a00}.bias{margin-top:14px}.note{font-size:12px;color:#687383;line-height:1.5}@media(max-width:650px){.grid{grid-template-columns:1fr}}</style></head><body><div class="wrap"><header><div><h1>🐂 BullBear AI</h1><div class="muted">Live Indian Market Radar</div></div><div id="status" class="status">Connecting…</div></header><div class="grid" id="cards"></div><div class="card bias"><div class="k">MARKET BIAS</div><div class="v wait">Waiting for live market data</div><div class="note">Live feed module is active. AI bias will be enabled after price, VIX, breadth, FII/DII and derivatives signals are added.</div></div></div><script>async function refresh(){try{const d=await fetch('/api/live').then(r=>r.json());document.getElementById('status').textContent=d.connection;
  const b=d.bias||{}; const el=document.getElementById('bias');
  el.textContent=b.label&&b.label!='WAITING'?`${b.label} — ${b.confidence}%`: 'Waiting for live market data';
  document.getElementById('biasReason').textContent=b.reason||'Collecting live ticks.';document.getElementById('cards').innerHTML=Object.entries(d.instruments).map(([n,x])=>{const ch=x.change_pct;const cls=ch==null?'wait':ch>=0?'pos':'neg';const c=ch==null?'Waiting':`${ch>=0?'+':''}${ch}%`;return `<div class="card"><div class="k">${n}</div><div class="v">${x.ltp==null?'—':Number(x.ltp).toLocaleString('en-IN')}</div><div class="${cls}">${c}</div></div>`}).join('')}catch(e){document.getElementById('status').textContent='Dashboard error'}}refresh();setInterval(refresh,1500)</script></body></html>''')
