import os
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import upstox_client
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="BullBear AI - Live Market Radar")
IST = ZoneInfo("Asia/Kolkata")

INSTRUMENTS = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "INDIA VIX": "NSE_INDEX|India VIX",
}

state = {
    name: {"instrument_key": key, "ltp": None, "change_pct": None, "last_update": None}
    for name, key in INSTRUMENTS.items()
}
state["connection"] = "starting"
state["error"] = None


def extract_ltp(feed):
    if not isinstance(feed, dict):
        return

    feeds = feed.get("feeds") or {}

    for name, key in INSTRUMENTS.items():
        item = feeds.get(key)
        if not item:
            continue

        ltpc = item.get("ltpc")
        if not ltpc:
            full_feed = item.get("fullFeed") or {}
            ltpc = (full_feed.get("marketFF") or {}).get("ltpc")

        if ltpc and ltpc.get("ltp") is not None:
            ltp = float(ltpc["ltp"])
            cp = ltpc.get("cp")

            change_pct = None
            if cp not in (None, 0):
                change_pct = round((ltp - float(cp)) / float(cp) * 100, 2)

            state[name].update(
                ltp=ltp,
                change_pct=change_pct,
                last_update=datetime.now(IST).isoformat(),
            )


def calculate_bias():
    n = state["NIFTY 50"]["change_pct"]
    b = state["BANK NIFTY"]["change_pct"]
    v = state["INDIA VIX"]["change_pct"]

    if n is None or b is None or v is None:
        return {
            "label": "WAITING",
            "confidence": 0,
            "score": 50,
            "reason": "Collecting live market data.",
        }

    score = 50.0
    score += max(-20, min(20, n * 8))
    score += max(-15, min(15, b * 6))
    score += max(-10, min(10, -v * 3))
    score = max(0, min(100, score))

    if score >= 60:
        label = "BULLISH"
    elif score <= 40:
        label = "BEARISH"
    else:
        label = "NEUTRAL"

    confidence = round(abs(score - 50) * 2, 1)

    reasons = [
        f"NIFTY {n:+.2f}%",
        f"BANK NIFTY {b:+.2f}%",
        f"INDIA VIX {v:+.2f}%",
    ]

    return {
        "label": label,
        "confidence": confidence,
        "score": round(score, 1),
        "reason": " | ".join(reasons),
    }


def start_streamer():
    token = os.getenv("UPSTOX_ACCESS_TOKEN")

    if not token:
        state["connection"] = "token_not_configured"
        return

    try:
        configuration = upstox_client.Configuration()
        configuration.access_token = token

        api_client = upstox_client.ApiClient(configuration)
        streamer = upstox_client.MarketDataStreamerV3(api_client)

        def on_open():
            state["connection"] = "connected"
            state["error"] = None
            streamer.subscribe(list(INSTRUMENTS.values()), "ltpc")

        def on_message(message):
            try:
                extract_ltp(message)
            except Exception as exc:
                state["error"] = f"feed_parse_error: {exc}"

        def on_error(error):
            state["connection"] = "error"
            state["error"] = str(error)

        def on_close(*args):
            state["connection"] = "closed"

        streamer.on("open", on_open)
        streamer.on("message", on_message)
        streamer.on("error", on_error)
        streamer.on("close", on_close)

        state["connection"] = "connecting"
        streamer.connect()

    except Exception as exc:
        state["connection"] = "error"
        state["error"] = str(exc)


@app.on_event("startup")
def startup():
    threading.Thread(target=start_streamer, daemon=True).start()


@app.get("/api/live")
def live():
    return {
        "connection": state["connection"],
        "error": state["error"],
        "instruments": {name: state[name] for name in INSTRUMENTS},
        "bias": calculate_bias(),
    }


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(
        """<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BullBear AI</title>
<style>
body{margin:0;background:#f4f6fa;color:#17202a;font-family:Arial,sans-serif}
.wrap{max-width:900px;margin:auto;padding:18px}
header{display:flex;justify-content:space-between;align-items:center;gap:12px}
h1{margin:0;font-size:23px}.muted{color:#687383;font-size:12px}
.status{padding:8px 10px;border-radius:9px;background:#fff;border:1px solid #dfe5ed;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:18px}
.card{background:#fff;border:1px solid #dfe5ed;border-radius:15px;padding:16px}
.k{font-size:11px;color:#687383;font-weight:bold}.v{font-size:26px;font-weight:800;margin:9px 0}
.pos{color:#168451}.neg{color:#b83a32}.wait{color:#9a6a00}
.bias{margin-top:14px}.reason{margin-top:8px;font-size:13px;color:#687383;line-height:1.5}
@media(max-width:650px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="wrap">
<header>
<div><h1>🐂 BullBear AI</h1><div class="muted">Live Indian Market Radar</div></div>
<div id="status" class="status">Connecting…</div>
</header>

<div class="grid" id="cards"></div>

<div class="card bias">
<div class="k">MARKET BIAS</div>
<div id="bias" class="v wait">Waiting for live market data</div>
<div id="confidence" class="muted"></div>
<div id="biasReason" class="reason">Collecting live ticks.</div>
</div>
</div>

<script>
async function refresh(){
  try{
    const response = await fetch('/api/live',{cache:'no-store'});
    if(!response.ok) throw new Error('API '+response.status);
    const d = await response.json();

    document.getElementById('status').textContent = d.connection || 'unknown';

    document.getElementById('cards').innerHTML =
      Object.entries(d.instruments).map(([name,x])=>{
        const ch=x.change_pct;
        const cls=ch==null?'wait':(ch>=0?'pos':'neg');
        const change=ch==null?'Waiting':((ch>=0?'+':'')+ch+'%');
        const price=x.ltp==null?'—':Number(x.ltp).toLocaleString('en-IN');
        return `<div class="card">
          <div class="k">${name}</div>
          <div class="v">${price}</div>
          <div class="${cls}">${change}</div>
        </div>`;
      }).join('');

    const b=d.bias || {};
    const el=document.getElementById('bias');
    el.textContent = b.label && b.label !== 'WAITING'
      ? `${b.label} — ${b.confidence}%`
      : 'Waiting for live market data';

    el.className='v ' + (b.label==='BULLISH'?'pos':b.label==='BEARISH'?'neg':'wait');
    document.getElementById('confidence').textContent =
      b.label && b.label !== 'WAITING' ? `Score: ${b.score}/100` : '';
    document.getElementById('biasReason').textContent =
      b.reason || 'Collecting live ticks.';

  }catch(e){
    document.getElementById('status').textContent='API connection problem';
    document.getElementById('bias').textContent='Waiting for live market data';
    document.getElementById('biasReason').textContent='Live data API could not be read.';
  }
}
refresh();
setInterval(refresh,1500);
</script>
</body>
</html>"""
    )
