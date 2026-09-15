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

app = FastAPI(title="BullBear AI")
IST = ZoneInfo("Asia/Kolkata")
UPSTOX = "https://api.upstox.com"

INSTRUMENTS = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "INDIA VIX": "NSE_INDEX|India VIX",
}

state = {
    name: {
        "instrument_key": key,
        "ltp": None,
        "change_pct": None,
        "cp": None,
        "volume": None,
        "last_update": None,
    }
    for name, key in INSTRUMENTS.items()
}

candles = {name: {"1m": [], "5m": []} for name in INSTRUMENTS}
analysis = {name: {} for name in INSTRUMENTS}
state["connection"] = "starting"
state["error"] = None
state["candle_status"] = "waiting"


def now_ist():
    return datetime.now(IST).isoformat()


def auth_headers():
    token = os.getenv("UPSTOX_ACCESS_TOKEN")
    if not token:
        return None
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


def price_vwap(rows):
    """
    Fallback reference for index candles when Upstox reports zero volume.
    This is an equal-weighted typical-price average, NOT traded-volume VWAP.
    """
    if not rows:
        return None
    return sum((r["high"] + r["low"] + r["close"]) / 3.0 for r in rows) / len(rows)


def update_from_feed(feed):
    if not isinstance(feed, dict):
        return
    feeds = feed.get("feeds") or {}

    for name, key in INSTRUMENTS.items():
        item = feeds.get(key)
        if not item:
            continue

        ltpc = item.get("ltpc")
        if not ltpc:
            full = item.get("fullFeed") or {}
            market_ff = full.get("marketFF") or {}
            ltpc = market_ff.get("ltpc")

        if not ltpc:
            continue

        ltp = float(ltpc["ltp"]) if ltpc.get("ltp") is not None else None
        cp = float(ltpc["cp"]) if ltpc.get("cp") is not None else None
        pct = None if ltp is None or cp in (None, 0) else round(((ltp - cp) / cp) * 100, 2)

        state[name].update(
            ltp=ltp,
            cp=cp,
            change_pct=pct,
            last_update=now_ist(),
        )


def start_websocket():
    token = os.getenv("UPSTOX_ACCESS_TOKEN")
    if not token:
        state["connection"] = "token_not_configured"
        state["error"] = "UPSTOX_ACCESS_TOKEN is missing"
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
                update_from_feed(message)
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


def fetch_candles(key, interval):
    headers = auth_headers()
    if not headers:
        return []

    url = f"{UPSTOX}/v3/historical-candle/intraday/{quote(key, safe='')}/minutes/{interval}"

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if not response.ok:
            state["candle_status"] = f"HTTP {response.status_code}"
            return []

        rows = ((response.json() or {}).get("data") or {}).get("candles") or []
        result = []

        for row in rows:
            if len(row) < 6:
                continue
            try:
                o, h, l, c = map(float, row[1:5])
            except (TypeError, ValueError):
                continue
            if any(v != v for v in (o, h, l, c)):
                continue
            result.append({
                "timestamp": row[0],
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": float(row[5]) if row[5] is not None else 0.0,
                "oi": float(row[6]) if len(row) > 6 and row[6] is not None else None,
            })

        result.sort(key=lambda x: x["timestamp"])

        # BullBear AI intentionally uses ONLY the current NSE session.
        # Do not carry previous-session candles into EMA50/RSI/VWAP.
        today_ist = datetime.now(IST).date()
        today_rows = []
        for row in result:
            try:
                ts = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=IST)
                if ts.astimezone(IST).date() == today_ist:
                    today_rows.append(row)
            except (TypeError, ValueError):
                continue

        return today_rows[-300:]
    except Exception as exc:
        state["candle_status"] = f"candle_error: {exc}"
        return []


def ema(values, period):
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    value = sum(values[:period]) / period
    for price in values[period:]:
        value = (price - value) * k + value
    return value


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains, losses = [], []
    for i in range(1, len(values)):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def vwap(rows):
    total_pv = 0.0
    total_volume = 0.0
    for row in rows:
        volume = row.get("volume") or 0.0
        typical = (row["high"] + row["low"] + row["close"]) / 3.0
        total_pv += typical * volume
        total_volume += volume

    return None if total_volume <= 0 else total_pv / total_volume


def market_structure(rows):
    if len(rows) < 6:
        return {"state": "INSUFFICIENT_DATA"}

    recent = rows[-6:]
    a, b = recent[:3], recent[3:]
    ah, al = max(x["high"] for x in a), min(x["low"] for x in a)
    bh, bl = max(x["high"] for x in b), min(x["low"] for x in b)

    if bh > ah and bl > al:
        return {"state": "BULLISH_STRUCTURE", "reason": "higher high + higher low"}
    if bh < ah and bl < al:
        return {"state": "BEARISH_STRUCTURE", "reason": "lower high + lower low"}
    return {"state": "MIXED_STRUCTURE", "reason": "structure is not aligned"}


def calculate_analysis(name):
    rows = candles[name]["5m"]

    if len(rows) < 20:
        analysis[name] = {
            "status": "waiting_for_more_candles",
            "timeframe": "5m",
            "candles": len(rows),
            "ema20_ready": False,
            "ema50_ready": len(rows) >= 50,
            "vwap_ready": False,
            "rsi14_ready": False,
            "bias": "WAITING",
            "confidence": 0,
        }
        return

    closes = [x["close"] for x in rows]
    current = closes[-1]
    e20, e50 = ema(closes, 20), ema(closes, 50)
    rsi14 = rsi(closes, 14)
    vw = vwap(rows)
    pvwap = None if vw is not None else price_vwap(rows)
    reference_vwap = vw if vw is not None else pvwap
    structure = market_structure(rows)

    score, reasons = 0, []

    if e20 is not None and e50 is not None:
        if e20 > e50:
            score += 2
            reasons.append("EMA20 above EMA50")
        elif e20 < e50:
            score -= 2
            reasons.append("EMA20 below EMA50")

    if reference_vwap is not None:
        if current > reference_vwap:
            score += 1
            reasons.append("price above VWAP reference")
        elif current < reference_vwap:
            score -= 1
            reasons.append("price below VWAP reference")

    if rsi14 is not None:
        if rsi14 >= 55:
            score += 1
            reasons.append("RSI momentum positive")
        elif rsi14 <= 45:
            score -= 1
            reasons.append("RSI momentum negative")

    if structure["state"] == "BULLISH_STRUCTURE":
        score += 2
        reasons.append("higher-high/higher-low structure")
    elif structure["state"] == "BEARISH_STRUCTURE":
        score -= 2
        reasons.append("lower-high/lower-low structure")

    bias = "BULLISH" if score >= 3 else "BEARISH" if score <= -3 else "NEUTRAL"
    confidence = min(100, round(50 + abs(score) * 10))

    analysis[name] = {
        "status": "ready",
        "timeframe": "5m",
        "candles": len(rows),
        "ema50_required_candles": 50,
        "ema20_ready": e20 is not None,
        "ema50_ready": e50 is not None,
        "vwap_ready": reference_vwap is not None,
        "rsi14_ready": rsi14 is not None,
        "price": round(current, 2),
        "ema20": round(e20, 2) if e20 is not None else None,
        "ema50": round(e50, 2) if e50 is not None else None,
        "vwap": round(reference_vwap, 2) if reference_vwap is not None else None,
        "vwap_type": "volume_vwap" if vw is not None else ("price_vwap_fallback" if pvwap is not None else None),
        "rsi14": round(rsi14, 2) if rsi14 is not None else None,
        "structure": structure,
        "score": score,
        "bias": bias,
        "confidence": confidence,
        "reasons": reasons,
        "note": "Analytical signal only; not a trading recommendation.",
    }


def refresh_data():
    while True:
        try:
            if not os.getenv("UPSTOX_ACCESS_TOKEN"):
                time.sleep(30)
                continue

            got_any = False
            for name, key in INSTRUMENTS.items():
                one = fetch_candles(key, 1)
                five = fetch_candles(key, 5)

                if one:
                    candles[name]["1m"] = one
                    got_any = True
                if five:
                    candles[name]["5m"] = five
                    got_any = True

                calculate_analysis(name)

            state["candle_status"] = "connected" if got_any else "no_candle_data"
        except Exception as exc:
            state["candle_status"] = f"error: {exc}"

        time.sleep(30)


@app.on_event("startup")
def startup():
    threading.Thread(target=start_websocket, daemon=True).start()
    threading.Thread(target=refresh_data, daemon=True).start()


@app.get("/api/market-data")
def market_data():
    return {
        "connection": state["connection"],
        "error": state["error"],
        "candle_status": state["candle_status"],
        "timestamp": now_ist(),
        "instruments": {name: dict(state[name]) for name in INSTRUMENTS},
    }


@app.get("/api/candles")
def candle_data():
    return {"timestamp": now_ist(), "status": state["candle_status"], "candles": candles}


@app.get("/api/analysis")
def analysis_data():
    return {"timestamp": now_ist(), "analysis": analysis}


@app.get("/api/options")
def options_data():
    """
    Option-chain phase scaffold. Real OI/PCR values are not fabricated.
    """
    return {
        "timestamp": now_ist(),
        "status": "not_configured",
        "underlyings": ["NIFTY 50", "BANK NIFTY"],
        "fields_planned": [
            "strike", "call_ltp", "put_ltp", "call_oi", "put_oi",
            "call_change_oi", "put_change_oi", "call_volume", "put_volume",
            "pcr_oi", "atm_strike"
        ],
        "note": "Real Upstox option-chain feed is not connected yet."
    }


@app.get("/api/diagnostics")
def diagnostics():
    return {
        "timestamp": now_ist(),
        "connection": state["connection"],
        "candle_status": state["candle_status"],
        "instruments": {
            name: {
                "1m_candles": len(candles[name]["1m"]),
                "5m_candles": len(candles[name]["5m"]),
                "ema50_required_candles": 50,
                "ema20_ready": analysis[name].get("ema20_ready", False),
                "ema50_ready": analysis[name].get("ema50_ready", False),
                "vwap_ready": analysis[name].get("vwap_ready", False),
                "vwap_type": analysis[name].get("vwap_type"),
                "rsi14_ready": analysis[name].get("rsi14_ready", False),
                "bias": analysis[name].get("bias", "WAITING"),
            }
            for name in INSTRUMENTS
        },
    }


@app.get("/api/live")
def live():
    return {**market_data(), "analysis": analysis}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse("""<!doctype html>
<html><head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BullBear AI</title>
<style>
body{margin:0;background:#f4f6fa;color:#17202a;font-family:Arial,sans-serif}
.wrap{max-width:1000px;margin:auto;padding:18px}
header{display:flex;justify-content:space-between;align-items:center;gap:12px}
h1{margin:0 0 4px;font-size:24px}.small{font-size:12px;color:#687383}
.status{background:#fff;border:1px solid #dfe5ed;border-radius:10px;padding:8px 10px;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:18px}
.card{background:#fff;border:1px solid #dfe5ed;border-radius:15px;padding:16px}
.label{font-size:11px;color:#687383;font-weight:bold}.price{font-size:27px;font-weight:800;margin:9px 0}
.green{color:#168451}.red{color:#b83a32}.orange{color:#9a6a00}.bias{font-size:21px;font-weight:800;margin:7px 0}
.metrics{display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:13px;margin-top:10px}
.metric{padding:7px;background:#f7f8fa;border-radius:8px}.section{margin-top:14px}
@media(max-width:700px){.grid{grid-template-columns:1fr}}
</style></head>
<body><div class="wrap">
<header><div><h1>🐂 BullBear AI</h1><div class="small">Live Indian Market Radar</div></div>
<div id="status" class="status">Connecting…</div></header>
<div id="cards" class="grid"></div>
<div class="card section"><div class="label">TECHNICAL ANALYSIS ENGINE</div>
<div id="engine" class="small">Waiting for candles…</div></div>
</div>
<script>
function fmt(v){return v==null?'—':Number(v).toLocaleString('en-IN',{maximumFractionDigits:2})}
async function refresh(){
 try{
  const d=await (await fetch('/api/live',{cache:'no-store'})).json();
  document.getElementById('status').textContent=d.connection;
  document.getElementById('cards').innerHTML=Object.entries(d.instruments).map(([name,x])=>{
   const a=d.analysis[name]||{}, c=x.change_pct;
   const cls=c==null?'orange':c>=0?'green':'red';
   const bcls=a.bias==='BULLISH'?'green':a.bias==='BEARISH'?'red':'orange';
   return `<div class="card"><div class="label">${name}</div>
    <div class="price">${fmt(x.ltp)}</div><div class="${cls}">${c==null?'Waiting':(c>=0?'+':'')+c+'%'}</div>
    <div class="section"><div class="label">5M MARKET BIAS</div>
    <div class="bias ${bcls}">${a.bias||'WAITING'}</div><div class="small">Confidence: ${a.confidence||0}%</div></div>
    <div class="metrics"><div class="metric">EMA20<br><b>${fmt(a.ema20)}</b></div>
    <div class="metric">EMA50<br><b>${fmt(a.ema50)}</b></div><div class="metric">VWAP<br><b>${fmt(a.vwap)}</b></div>
    <div class="metric">RSI14<br><b>${fmt(a.rsi14)}</b></div></div></div>`;
  }).join('');
  const ready=Object.values(d.analysis||{}).filter(x=>x.status==='ready');
  const ema50ok=ready.filter(x=>x.ema50_ready).length;
  document.getElementById('engine').textContent=ready.length
   ? `LIVE 5M ONLY • EMA20/50 • VWAP • RSI14 • Market Structure • Bias ACTIVE • EMA50 ready ${ema50ok}/${ready.length}`
   : 'Collecting today’s live 5-minute candles…';
 }catch(e){document.getElementById('status').textContent='API error'}
}
refresh();setInterval(refresh,2000);
</script></body></html>""")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
