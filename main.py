import os, json
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

load_dotenv()
app = FastAPI(title="AI Market Radar V4")

# V4 starter: the browser dashboard is ready; live WebSocket wiring is isolated
# in the backend so the Upstox token never needs to be exposed to the browser.
INSTRUMENTS = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "INDIA VIX": "NSE_INDEX|India VIX",
}

state = {
    name: {"instrument_key": key, "ltp": None, "change_pct": None}
    for name, key in INSTRUMENTS.items()
}

@app.get("/api/status")
def status():
    token_set = bool(os.getenv("UPSTOX_ACCESS_TOKEN"))
    return {
        "ok": True,
        "token_configured": token_set,
        "timezone": "Asia/Kolkata",
        "updated": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "instruments": state,
        "next": "Connect MarketDataStreamerV3 in the server process."
    }

@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse("""<!doctype html>
<html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AI Market Radar V4</title>
<style>
body{font-family:Arial;margin:0;background:#f4f6fa;color:#17202a}.wrap{max-width:1050px;margin:auto;padding:18px}
header{display:flex;justify-content:space-between;gap:12px;align-items:center}h1{margin:0}.muted{color:#697586;font-size:13px}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:18px}.card{background:#fff;border:1px solid #dfe5ed;border-radius:15px;padding:16px}
.k{font-size:11px;color:#687383;font-weight:bold}.v{font-size:26px;font-weight:800;margin:8px 0}.status{padding:9px 12px;background:#fff;border:1px solid #dfe5ed;border-radius:10px;font-size:12px}
.bias{margin-top:14px}.bar{height:12px;background:#e9edf3;border-radius:12px;overflow:hidden}.fill{width:50%;height:100%}
@media(max-width:700px){.grid{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<header><div><h1>AI Market Radar V4</h1><div class="muted">Live-data foundation • Upstox backend</div></div><div id="conn" class="status">Checking…</div></header>
<div class="grid" id="cards"></div>
<div class="card bias"><div class="k">AI MARKET BIAS</div><div class="v" id="bias">Waiting for live data</div>
<div class="bar"><div class="fill" id="fill"></div></div><p class="muted">Bias will be calculated only after live inputs arrive. No fabricated market values.</p></div>
</div>
<script>
async function refresh(){
 const r=await fetch('/api/status'); const d=await r.json();
 document.getElementById('conn').textContent=d.token_configured?'Token configured':'Token not configured';
 const cards=Object.entries(d.instruments).map(([n,x])=>`<div class="card"><div class="k">${n}</div><div class="v">${x.ltp??'—'}</div><div class="muted">${x.change_pct==null?'Waiting for tick':x.change_pct+'%'}</div></div>`).join('');
 document.getElementById('cards').innerHTML=cards;
}
refresh(); setInterval(refresh,2000);
</script></body></html>""")

# Run with:
#   copy .env.example to .env
#   put your token in .env locally
#   pip install -r requirements.txt
#   uvicorn main:app --host 127.0.0.1 --port 8000
