import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote

import requests
import upstox_client
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title='BullBear AI')
IST = ZoneInfo('Asia/Kolkata')
BASE = 'https://api.upstox.com'

INSTRUMENTS = {
    'NIFTY 50': 'NSE_INDEX|Nifty 50',
    'BANK NIFTY': 'NSE_INDEX|Nifty Bank',
    'INDIA VIX': 'NSE_INDEX|India VIX',
}

state = {name: {'instrument_key': key, 'ltp': None, 'change_pct': None,
                'cp': None, 'volume': None, 'last_update': None}
         for name, key in INSTRUMENTS.items()}
state.update({'connection': 'starting', 'error': None, 'candle_status': 'waiting'})
candles = {name: {'1m': [], '5m': []} for name in INSTRUMENTS}


def now_ist():
    return datetime.now(IST).isoformat()


def headers():
    token = os.getenv('UPSTOX_ACCESS_TOKEN')
    return ({'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
            if token else None)


def update_feed(message):
    feeds = (message or {}).get('feeds', {})
    for name, key in INSTRUMENTS.items():
        item = feeds.get(key)
        if not item:
            continue
        ltpc = item.get('ltpc')
        if not ltpc:
            ltpc = ((item.get('fullFeed') or {}).get('marketFF') or {}).get('ltpc')
        if not ltpc:
            continue
        ltp = float(ltpc['ltp']) if ltpc.get('ltp') is not None else None
        cp = float(ltpc['cp']) if ltpc.get('cp') is not None else None
        pct = round((ltp - cp) / cp * 100, 2) if ltp is not None and cp else None
        state[name].update(ltp=ltp, cp=cp, change_pct=pct, last_update=now_ist())


def websocket_worker():
    token = os.getenv('UPSTOX_ACCESS_TOKEN')
    if not token:
        state['connection'] = 'token_not_configured'
        state['error'] = 'UPSTOX_ACCESS_TOKEN is missing'
        return
    try:
        cfg = upstox_client.Configuration()
        cfg.access_token = token
        client = upstox_client.ApiClient(cfg)
        streamer = upstox_client.MarketDataStreamerV3(client)

        def opened():
            state['connection'] = 'connected'
            state['error'] = None
            streamer.subscribe(list(INSTRUMENTS.values()), 'ltpc')

        streamer.on('open', opened)
        streamer.on('message', lambda msg: update_feed(msg))
        streamer.on('error', lambda err: state.update(connection='error', error=str(err)))
        streamer.on('close', lambda *args: state.update(connection='closed'))
        state['connection'] = 'connecting'
        streamer.connect()
    except Exception as exc:
        state['connection'] = 'error'
        state['error'] = str(exc)


def fetch_candles(key, minutes):
    h = headers()
    if not h:
        return []
    url = f"{BASE}/v3/historical-candle/intraday/{quote(key, safe='')}/minutes/{minutes}"
    try:
        r = requests.get(url, headers=h, timeout=10)
        if not r.ok:
            state['candle_status'] = f'HTTP {r.status_code}'
            return []
        rows = ((r.json().get('data') or {}).get('candles') or [])
        out = []
        for row in rows:
            if len(row) >= 6:
                out.append({'timestamp': row[0], 'open': row[1], 'high': row[2],
                            'low': row[3], 'close': row[4], 'volume': row[5],
                            'oi': row[6] if len(row) > 6 else None})
        return out
    except Exception as exc:
        state['candle_status'] = f'candle_error: {exc}'
        return []


def candle_worker():
    while True:
        try:
            got = False
            for name, key in INSTRUMENTS.items():
                c1 = fetch_candles(key, 1)
                c5 = fetch_candles(key, 5)
                if c1:
                    candles[name]['1m'] = c1[-120:]
                    got = True
                if c5:
                    candles[name]['5m'] = c5[-120:]
                    got = True
            state['candle_status'] = 'connected' if got else 'no_candle_data'
        except Exception as exc:
            state['candle_status'] = f'error: {exc}'
        time.sleep(30)


@app.on_event('startup')
def startup():
    threading.Thread(target=websocket_worker, daemon=True).start()
    threading.Thread(target=candle_worker, daemon=True).start()


@app.get('/api/market-data')
def market_data():
    return {'connection': state['connection'], 'error': state['error'],
            'candle_status': state['candle_status'], 'timestamp': now_ist(),
            'instruments': {n: dict(state[n]) for n in INSTRUMENTS}}


@app.get('/api/candles')
def candle_data():
    return {'timestamp': now_ist(), 'status': state['candle_status'], 'candles': candles}


@app.get('/api/live')
def live():
    return market_data()


@app.get('/', response_class=HTMLResponse)
def dashboard():
    return HTMLResponse('''<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>BullBear AI</title><style>body{margin:0;background:#f4f6fa;color:#17202a;font-family:Arial}.wrap{max-width:950px;margin:auto;padding:18px}header{display:flex;justify-content:space-between;align-items:center}.status,.card{background:#fff;border:1px solid #dfe5ed;border-radius:15px}.status{padding:8px 10px}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:18px}.card{padding:16px}.label{font-size:11px;color:#687383;font-weight:bold}.price{font-size:27px;font-weight:800;margin:9px 0}.green{color:#168451}.red{color:#b83a32}.orange{color:#9a6a00}.box{margin-top:14px}@media(max-width:650px){.grid{grid-template-columns:1fr}}</style></head><body><div class="wrap"><header><div><h1>🐂 BullBear AI</h1><div>Live Indian Market Radar</div></div><div id="status" class="status">Connecting…</div></header><div id="cards" class="grid"></div><div class="card box"><div class="label">DATA COLLECTOR</div><div id="collector" class="price orange">Starting…</div><div>Upstox WebSocket + V3 intraday candles</div></div></div><script>async function refresh(){try{const d=await (await fetch('/api/market-data',{cache:'no-store'})).json();document.getElementById('status').textContent=d.connection;document.getElementById('cards').innerHTML=Object.entries(d.instruments).map(([n,x])=>{let c=x.change_pct,cl=c==null?'orange':c>=0?'green':'red',p=c==null?'Waiting':(c>=0?'+':'')+c+'%';return `<div class="card"><div class="label">${n}</div><div class="price">${x.ltp==null?'—':Number(x.ltp).toLocaleString('en-IN')}</div><div class="${cl}">${p}</div><div>Volume: ${x.volume==null?'—':Number(x.volume).toLocaleString('en-IN')}</div></div>`}).join('');let ok=d.connection==='connected',ck=d.candle_status==='connected';let e=document.getElementById('collector');e.textContent=ok&&ck?'Live + Candles Connected':ok?'Live Connected • Candles '+d.candle_status:'Collector '+d.connection;e.className='price '+(ok&&ck?'green':'orange')}catch(e){document.getElementById('status').textContent='API error';document.getElementById('collector').textContent='API error'}}refresh();setInterval(refresh,1500)</script></body></html>''')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('PORT', '8000')))
