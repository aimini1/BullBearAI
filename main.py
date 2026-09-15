import os
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import upstox_client
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title='BullBear AI')
IST = ZoneInfo('Asia/Kolkata')

INSTRUMENTS = {
    'NIFTY 50': 'NSE_INDEX|Nifty 50',
    'BANK NIFTY': 'NSE_INDEX|Nifty Bank',
    'INDIA VIX': 'NSE_INDEX|India VIX',
}

state = {
    name: {
        'instrument_key': key,
        'ltp': None,
        'change_pct': None,
        'cp': None,
        'volume': None,
        'last_update': None,
    }
    for name, key in INSTRUMENTS.items()
}
state['connection'] = 'starting'
state['error'] = None


def now_ist():
    return datetime.now(IST).isoformat()


def update_from_feed(feed):
    if not isinstance(feed, dict):
        return
    feeds = feed.get('feeds') or {}
    for name, key in INSTRUMENTS.items():
        item = feeds.get(key)
        if not item:
            continue
        ltpc = item.get('ltpc')
        if not ltpc:
            full = item.get('fullFeed') or {}
            ltpc = (full.get('marketFF') or {}).get('ltpc')
        if not ltpc:
            continue
        ltp = ltpc.get('ltp')
        cp = ltpc.get('cp')
        ltp = float(ltp) if ltp is not None else None
        cp = float(cp) if cp is not None else None
        pct = None if ltp is None or cp in (None, 0) else round((ltp - cp) / cp * 100, 2)
        state[name].update(ltp=ltp, cp=cp, change_pct=pct, last_update=now_ist())


def start_websocket():
    token = os.getenv('UPSTOX_ACCESS_TOKEN')
    if not token:
        state['connection'] = 'token_not_configured'
        state['error'] = 'UPSTOX_ACCESS_TOKEN is missing'
        return
    try:
        configuration = upstox_client.Configuration()
        configuration.access_token = token
        api_client = upstox_client.ApiClient(configuration)
        streamer = upstox_client.MarketDataStreamerV3(api_client)

        def on_open():
            state['connection'] = 'connected'
            state['error'] = None
            streamer.subscribe(list(INSTRUMENTS.values()), 'ltpc')

        def on_message(message):
            try:
                update_from_feed(message)
            except Exception as exc:
                state['error'] = f'feed_parse_error: {exc}'

        def on_error(error):
            state['connection'] = 'error'
            state['error'] = str(error)

        def on_close(*args):
            state['connection'] = 'closed'

        streamer.on('open', on_open)
        streamer.on('message', on_message)
        streamer.on('error', on_error)
        streamer.on('close', on_close)
        state['connection'] = 'connecting'
        streamer.connect()
    except Exception as exc:
        state['connection'] = 'error'
        state['error'] = str(exc)


def quote_snapshot():
    token = os.getenv('UPSTOX_ACCESS_TOKEN')
    if not token:
        return
    try:
        response = requests.get(
            'https://api.upstox.com/v2/market-quote/ltp',
            headers={'Accept': 'application/json', 'Authorization': f'Bearer {token}'},
            params={'instrument_key': ','.join(INSTRUMENTS.values())},
            timeout=8,
        )
        if not response.ok:
            return
        data = (response.json() or {}).get('data') or {}
        for key, quote in data.items():
            name = next((n for n, k in INSTRUMENTS.items() if k == key), None)
            if not name:
                continue
            ltp = quote.get('last_price')
            cp = quote.get('cp')
            volume = quote.get('volume')
            ltp = float(ltp) if ltp is not None else None
            cp = float(cp) if cp is not None else None
            pct = None if ltp is None or cp in (None, 0) else round((ltp - cp) / cp * 100, 2)
            state[name].update(
                ltp=ltp if ltp is not None else state[name]['ltp'],
                cp=cp if cp is not None else state[name]['cp'],
                change_pct=pct if pct is not None else state[name]['change_pct'],
                volume=volume,
                last_update=now_ist(),
            )
    except Exception:
        pass


def collector_loop():
    while True:
        quote_snapshot()
        threading.Event().wait(30)


@app.on_event('startup')
def startup():
    threading.Thread(target=start_websocket, daemon=True).start()
    threading.Thread(target=collector_loop, daemon=True).start()


@app.get('/api/market-data')
def market_data():
    return {
        'connection': state['connection'],
        'error': state['error'],
        'timestamp': now_ist(),
        'instruments': {name: dict(state[name]) for name in INSTRUMENTS},
    }


@app.get('/api/live')
def live():
    return market_data()


@app.get('/', response_class=HTMLResponse)
def dashboard():
    return HTMLResponse('''<!doctype html>
<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BullBear AI</title>
<style>
body{margin:0;background:#f4f6fa;color:#17202a;font-family:Arial,sans-serif}.wrap{max-width:950px;margin:auto;padding:18px}
header{display:flex;justify-content:space-between;align-items:center;gap:12px}h1{margin:0 0 4px;font-size:24px}.small{font-size:12px;color:#687383}
.status{background:#fff;border:1px solid #dfe5ed;border-radius:10px;padding:8px 10px;font-size:12px}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:18px}
.card{background:#fff;border:1px solid #dfe5ed;border-radius:15px;padding:16px}.label{font-size:11px;color:#687383;font-weight:bold}.price{font-size:27px;font-weight:800;margin:9px 0}
.green{color:#168451}.red{color:#b83a32}.orange{color:#9a6a00}.box{margin-top:14px}@media(max-width:650px){.grid{grid-template-columns:1fr}}
</style></head><body><div class="wrap"><header><div><h1>🐂 BullBear AI</h1><div class="small">Live Indian Market Radar</div></div><div id="status" class="status">Connecting…</div></header>
<div id="cards" class="grid"></div><div class="card box"><div class="label">DATA COLLECTOR</div><div id="collector" class="price orange">Starting…</div><div class="small">Upstox WebSocket + quote snapshot</div></div></div>
<script>
async function refresh(){try{const r=await fetch('/api/market-data',{cache:'no-store'});const d=await r.json();document.getElementById('status').textContent=d.connection;
document.getElementById('cards').innerHTML=Object.entries(d.instruments).map(([name,x])=>{const c=x.change_pct;const cls=c==null?'orange':c>=0?'green':'red';const pct=c==null?'Waiting':(c>=0?'+':'')+c+'%';const vol=x.volume==null?'—':Number(x.volume).toLocaleString('en-IN');return `<div class="card"><div class="label">${name}</div><div class="price">${x.ltp==null?'—':Number(x.ltp).toLocaleString('en-IN')}</div><div class="${cls}">${pct}</div><div class="small">Volume: ${vol}</div></div>`}).join('');
const ok=d.connection==='connected';const el=document.getElementById('collector');el.textContent=ok?'Collector Connected':'Collector '+d.connection;el.className='price '+(ok?'green':'orange');}catch(e){document.getElementById('status').textContent='API error';document.getElementById('collector').textContent='API error';}}
refresh();setInterval(refresh,1500);
</script></body></html>''')

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('PORT','8000')))
