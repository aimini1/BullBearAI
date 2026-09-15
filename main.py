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

OPTIONS_UNDERLYINGS = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
}

# NIFTY 50 has the current weekly option chain.
# BANK NIFTY uses the current monthly chain because its weekly index
# options are no longer the active weekly series.
OPTION_EXPIRIES = {
    "NIFTY 50": "current_week",
    "BANK NIFTY": "current_month",
}

option_chains = {
    name: {
        "status": "waiting",
        "expiry": "current_week",
        "spot": None,
        "atm_strike": None,
        "count": 0,
        "data": [],
        "updated_at": None,
        "error": None,
    }
    for name in OPTIONS_UNDERLYINGS
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


def fetch_option_chain(name, expiry=None):
    """Fetch the current Upstox put/call option chain for an index."""
    if expiry is None:
        expiry = OPTION_EXPIRIES.get(name, "current_week")
    headers = auth_headers()
    if not headers:
        return []

    key = OPTIONS_UNDERLYINGS[name]
    url = f"{UPSTOX}/v2/option/chain"

    try:
        response = requests.get(
            url,
            headers=headers,
            params={"instrument_key": key, "expiry_date": expiry},
            timeout=12,
        )

        payload = response.json() if response.content else {}
        if not response.ok or payload.get("status") != "success":
            option_chains[name].update(
                status=f"HTTP {response.status_code}",
                error=payload.get("errors") or payload.get("message") or "option chain request failed",
                updated_at=now_ist(),
            )
            return []

        rows = payload.get("data") or []
        clean = []

        for row in rows:
            try:
                strike = float(row.get("strike_price"))
            except (TypeError, ValueError):
                continue

            call = row.get("call_options") or {}
            put = row.get("put_options") or {}
            call_md = call.get("market_data") or {}
            put_md = put.get("market_data") or {}
            call_g = call.get("option_greeks") or {}
            put_g = put.get("option_greeks") or {}

            clean.append({
                "expiry": row.get("expiry"),
                "strike": strike,
                "spot": row.get("underlying_spot_price"),
                "pcr": row.get("pcr"),
                "call": {
                    "instrument_key": call.get("instrument_key"),
                    "ltp": call_md.get("ltp"),
                    "volume": call_md.get("volume"),
                    "oi": call_md.get("oi"),
                    "prev_oi": call_md.get("prev_oi"),
                    "change_oi": (
                        (call_md.get("oi") - call_md.get("prev_oi"))
                        if call_md.get("oi") is not None and call_md.get("prev_oi") is not None
                        else None
                    ),
                    "iv": call_g.get("iv", call_md.get("iv")),
                    "delta": call_g.get("delta"),
                    "gamma": call_g.get("gamma"),
                    "theta": call_g.get("theta"),
                    "vega": call_g.get("vega"),
                    "pop": call_g.get("pop"),
                },
                "put": {
                    "instrument_key": put.get("instrument_key"),
                    "ltp": put_md.get("ltp"),
                    "volume": put_md.get("volume"),
                    "oi": put_md.get("oi"),
                    "prev_oi": put_md.get("prev_oi"),
                    "change_oi": (
                        (put_md.get("oi") - put_md.get("prev_oi"))
                        if put_md.get("oi") is not None and put_md.get("prev_oi") is not None
                        else None
                    ),
                    "iv": put_g.get("iv", put_md.get("iv")),
                    "delta": put_g.get("delta"),
                    "gamma": put_g.get("gamma"),
                    "theta": put_g.get("theta"),
                    "vega": put_g.get("vega"),
                    "pop": put_g.get("pop"),
                },
            })

        clean.sort(key=lambda x: x["strike"])
        spot = clean[0]["spot"] if clean else None

        if spot is not None and clean:
            atm = min(clean, key=lambda x: abs(x["strike"] - float(spot)))
            atm_strike = atm["strike"]
        else:
            atm_strike = None

        option_chains[name].update(
            status="success",
            expiry=(clean[0].get("expiry") if clean else expiry),
            spot=spot,
            atm_strike=atm_strike,
            count=len(clean),
            data=clean,
            updated_at=now_ist(),
            error=None,
        )
        return clean

    except Exception as exc:
        option_chains[name].update(
            status="error",
            error=str(exc),
            updated_at=now_ist(),
        )
        return []



def option_chain_analysis(name):
    """
    Descriptive option-chain metrics from the latest fetched chain.
    These are analytical references, not trading recommendations.
    """
    rows = option_chains[name].get("data") or []
    if not rows:
        return {
            "status": "waiting",
            "total_call_oi": None,
            "total_put_oi": None,
            "pcr_oi": None,
            "max_call_oi_strike": None,
            "max_put_oi_strike": None,
            "call_oi_change": None,
            "put_oi_change": None,
            "oi_reference_resistance": None,
            "oi_reference_support": None,
            "options_bias": "WAITING",
            "note": "No option-chain data available.",
        }

    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    total_call_oi = sum(num((r.get("call") or {}).get("oi")) for r in rows)
    total_put_oi = sum(num((r.get("put") or {}).get("oi")) for r in rows)

    call_oi_change = sum(
        num((r.get("call") or {}).get("change_oi")) for r in rows
    )
    put_oi_change = sum(
        num((r.get("put") or {}).get("change_oi")) for r in rows
    )

    call_wall = max(
        rows,
        key=lambda r: num((r.get("call") or {}).get("oi")),
        default=None,
    )
    put_wall = max(
        rows,
        key=lambda r: num((r.get("put") or {}).get("oi")),
        default=None,
    )

    pcr = (total_put_oi / total_call_oi) if total_call_oi > 0 else None

    # Keep the label deliberately descriptive: OI/PCR alone is not a
    # reliable directional trading signal.
    if pcr is None:
        options_bias = "WAITING"
    elif pcr >= 1.20:
        options_bias = "PUT-OI HEAVY"
    elif pcr <= 0.80:
        options_bias = "CALL-OI HEAVY"
    else:
        options_bias = "BALANCED"

    return {
        "status": "ready",
        "total_call_oi": round(total_call_oi, 2),
        "total_put_oi": round(total_put_oi, 2),
        "pcr_oi": round(pcr, 3) if pcr is not None else None,
        "max_call_oi_strike": call_wall.get("strike") if call_wall else None,
        "max_put_oi_strike": put_wall.get("strike") if put_wall else None,
        "max_call_oi": num((call_wall.get("call") or {}).get("oi")) if call_wall else None,
        "max_put_oi": num((put_wall.get("put") or {}).get("oi")) if put_wall else None,
        "call_oi_change": round(call_oi_change, 2),
        "put_oi_change": round(put_oi_change, 2),
        "oi_reference_resistance": call_wall.get("strike") if call_wall else None,
        "oi_reference_support": put_wall.get("strike") if put_wall else None,
        "options_bias": options_bias,
        "note": "Descriptive OI/PCR analysis only; not a trading recommendation.",
    }


def option_chain_snapshot(name, strikes_each_side=5):
    x = option_chains[name]
    rows = x.get("data") or []
    atm = x.get("atm_strike")

    if atm is None or not rows:
        return {
            "status": x.get("status"),
            "expiry": x.get("expiry"),
            "spot": x.get("spot"),
            "atm_strike": None,
            "count": x.get("count", 0),
            "updated_at": x.get("updated_at"),
            "error": x.get("error"),
            "analysis": option_chain_analysis(name),
            "data": [],
        }

    ordered = sorted(rows, key=lambda r: abs(r["strike"] - atm))
    selected_strikes = sorted(
        r["strike"] for r in ordered[: 2 * strikes_each_side + 1]
    )
    selected = [r for r in rows if r["strike"] in selected_strikes]

    return {
        "status": x.get("status"),
        "expiry": x.get("expiry"),
        "spot": x.get("spot"),
        "atm_strike": atm,
        "count": x.get("count", 0),
        "updated_at": x.get("updated_at"),
        "error": x.get("error"),
        "analysis": option_chain_analysis(name),
        "data": selected,
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

            for option_name in OPTIONS_UNDERLYINGS:
                fetch_option_chain(option_name, OPTION_EXPIRIES.get(option_name, "current_week"))

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


@app.get("/api/options-chain/{underlying}")
def options_chain_api(underlying: str):
    name = underlying.strip().upper()
    aliases = {
        "NIFTY": "NIFTY 50",
        "NIFTY 50": "NIFTY 50",
        "BANKNIFTY": "BANK NIFTY",
        "BANK NIFTY": "BANK NIFTY",
    }
    name = aliases.get(name)
    if name not in OPTIONS_UNDERLYINGS:
        return {
            "status": "error",
            "detail": "Use NIFTY 50 or BANK NIFTY",
        }

    snapshot = option_chain_snapshot(name, strikes_each_side=5)
    snapshot["requested_expiry"] = OPTION_EXPIRIES.get(name, "current_week")
    return snapshot


@app.get("/api/options-analysis/{underlying}")
def options_analysis_api(underlying: str):
    name = underlying.strip().upper()
    aliases = {
        "NIFTY": "NIFTY 50",
        "NIFTY 50": "NIFTY 50",
        "BANKNIFTY": "BANK NIFTY",
        "BANK NIFTY": "BANK NIFTY",
    }
    name = aliases.get(name)
    if name not in OPTIONS_UNDERLYINGS:
        return {"status": "error", "detail": "Use NIFTY 50 or BANK NIFTY"}
    return option_chain_analysis(name)


@app.get("/api/options-chain")
def options_chain_all():
    result = {}
    for name in OPTIONS_UNDERLYINGS:
        snapshot = option_chain_snapshot(name, strikes_each_side=5)
        snapshot["requested_expiry"] = OPTION_EXPIRIES.get(name, "current_week")
        result[name] = snapshot

    return {
        "timestamp": now_ist(),
        "underlyings": result,
        "analysis": {
            name: option_chain_analysis(name)
            for name in OPTIONS_UNDERLYINGS
        },
    }


@app.get("/api/live")
def live():
    return {**market_data(), "analysis": analysis}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse("""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0b1020">
<title>BullBear AI</title>
<style>
:root{
  --bg:#080c16;--panel:#101625;--panel2:#151c2d;--line:#222c40;
  --text:#f4f7fb;--muted:#8f9bb0;--green:#31d08b;--red:#ff6474;
  --yellow:#f4c95d;--blue:#65a9ff;--white:#fff;
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{
  margin:0;background:radial-gradient(circle at 50% -10%,#17223a 0,#080c16 38%);
  color:var(--text);font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
}
.wrap{width:min(1080px,100%);margin:auto;padding:14px 12px 34px}
.topbar{
  position:sticky;top:0;z-index:20;margin:-14px -12px 14px;padding:13px 14px;
  background:rgba(8,12,22,.90);backdrop-filter:blur(14px);border-bottom:1px solid var(--line);
  display:flex;align-items:center;justify-content:space-between;gap:12px;
}
.brand{display:flex;align-items:center;gap:10px}
.logo{
  width:40px;height:40px;border-radius:13px;display:grid;place-items:center;
  background:linear-gradient(135deg,#1d2c4a,#0f1728);border:1px solid #30405e;font-size:21px
}
h1{font-size:18px;line-height:1;margin:0 0 4px;font-weight:800}
.subtitle{font-size:10px;color:var(--muted);letter-spacing:.5px}
.live{
  display:flex;align-items:center;gap:7px;padding:8px 10px;border:1px solid var(--line);
  border-radius:999px;background:#0e1422;font-size:10px;color:var(--muted);white-space:nowrap
}
.dot{width:7px;height:7px;border-radius:50%;background:var(--yellow);box-shadow:0 0 10px rgba(244,201,93,.55)}
.dot.ok{background:var(--green);box-shadow:0 0 10px rgba(49,208,139,.6)}
.hero{
  border:1px solid var(--line);border-radius:20px;padding:16px;margin-bottom:12px;
  background:linear-gradient(145deg,#121a2b,#0e1421);box-shadow:0 14px 40px rgba(0,0,0,.22)
}
.hero-title{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1px}
.hero-row{display:flex;align-items:end;justify-content:space-between;gap:12px;margin-top:6px}
.hero-big{font-size:25px;font-weight:850}
.hero-note{font-size:10px;color:var(--muted);text-align:right}
.market-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.card{
  background:rgba(16,22,37,.96);border:1px solid var(--line);border-radius:18px;padding:14px;
  box-shadow:0 10px 30px rgba(0,0,0,.16)
}
.card-head{display:flex;justify-content:space-between;align-items:center;gap:8px}
.label{font-size:10px;color:var(--muted);font-weight:800;letter-spacing:.8px;text-transform:uppercase}
.price{font-size:26px;font-weight:850;margin:9px 0 2px;letter-spacing:-.5px}
.change{font-size:12px;font-weight:750}
.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}.blue{color:var(--blue)}
.badge{
  padding:5px 8px;border-radius:999px;font-size:9px;font-weight:850;letter-spacing:.5px;
  border:1px solid currentColor;background:rgba(255,255,255,.025)
}
.bias{font-size:19px;font-weight:900;margin:7px 0 2px}
.conf{font-size:10px;color:var(--muted)}
.metrics{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-top:12px}
.metric{background:var(--panel2);border:1px solid #1c2639;border-radius:11px;padding:8px}
.metric span{display:block;color:var(--muted);font-size:9px;margin-bottom:4px}
.metric b{font-size:12px}
.section{margin-top:12px}
.section-title{
  display:flex;align-items:center;justify-content:space-between;gap:8px;margin:18px 0 9px
}
.section-title h2{font-size:13px;margin:0;font-weight:850}
.section-title span{font-size:9px;color:var(--muted)}
.option-wrap{overflow:hidden;border:1px solid var(--line);border-radius:18px;background:var(--panel)}
.option-head{padding:14px;border-bottom:1px solid var(--line)}
.option-meta{font-size:11px;color:var(--muted);margin-top:5px}
.option-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:10px}
.stat{background:var(--panel2);border-radius:10px;padding:8px}
.stat span{display:block;font-size:8px;color:var(--muted);margin-bottom:4px}
.stat b{font-size:11px}
.table-scroll{overflow:auto;-webkit-overflow-scrolling:touch}
table{width:100%;min-width:520px;border-collapse:collapse;font-size:10px}
th{
  position:sticky;top:0;background:#0c1220;color:var(--muted);font-size:8px;
  letter-spacing:.5px;padding:9px 7px;text-align:right;white-space:nowrap
}
th:nth-child(3),td:nth-child(3){text-align:center}
td{padding:8px 7px;border-top:1px solid #1c2537;text-align:right;white-space:nowrap}
tr.atm td{background:rgba(244,201,93,.10);font-weight:850;color:#ffe59a}
.wall{font-size:9px;color:var(--muted);margin-top:8px}
.engine{
  display:grid;grid-template-columns:repeat(3,1fr);gap:8px
}
.engine-item{padding:11px;border:1px solid var(--line);border-radius:12px;background:var(--panel)}
.engine-item b{display:block;font-size:11px;margin-bottom:4px}
.engine-item span{font-size:9px;color:var(--muted);line-height:1.35}
.footer-note{font-size:9px;color:#68758b;text-align:center;margin-top:18px;line-height:1.5}
.error{color:var(--red);font-size:10px;margin-top:7px}
.empty{padding:18px;color:var(--muted);font-size:11px;text-align:center}
@media(max-width:760px){
  .market-grid{grid-template-columns:1fr}
  .hero-big{font-size:23px}
  .option-stats{grid-template-columns:1fr 1fr}
  .engine{grid-template-columns:1fr}
  .card{padding:13px}
}
@media(min-width:761px){
  .market-grid .card:nth-child(1){grid-column:span 1}
  .market-grid .card:nth-child(2){grid-column:span 1}
  .market-grid .card:nth-child(3){grid-column:span 1}
}
</style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div class="brand">
      <div class="logo">🐂</div>
      <div><h1>BullBear AI</h1><div class="subtitle">LIVE INDIAN MARKET RADAR</div></div>
    </div>
    <div class="live"><i id="dot" class="dot"></i><span id="status">CONNECTING…</span></div>
  </div>

  <div class="hero">
    <div class="hero-title">Market dashboard</div>
    <div class="hero-row">
      <div class="hero-big">Technical + Options</div>
      <div class="hero-note">Live 5M engine<br><span id="clock">—</span></div>
    </div>
  </div>

  <div id="cards" class="market-grid"></div>

  <div class="section-title">
    <h2>OPTION CHAIN</h2><span>LIVE ANALYSIS</span>
  </div>
  <div id="options"></div>

  <div class="section-title">
    <h2>SIGNAL ENGINE</h2><span>TECHNICAL + OPTIONS</span>
  </div>
  <div id="signals" class="engine"></div>

  <div class="footer-note">
    BullBear AI provides descriptive market analytics only. It is not a trading recommendation.
  </div>
</div>

<script>
const $=id=>document.getElementById(id);
function fmt(v){
  if(v===null||v===undefined||v==='') return '—';
  const n=Number(v);
  return Number.isFinite(n)?n.toLocaleString('en-IN',{maximumFractionDigits:2}):String(v);
}
function biasClass(v){return v==='BULLISH'?'green':v==='BEARISH'?'red':'yellow'}
function biasBadge(v){
  const c=biasClass(v||'WAITING');
  return `<span class="badge ${c}">${v||'WAITING'}</span>`;
}
function renderCards(instruments, analyses){
  $('cards').innerHTML=Object.entries(instruments).map(([name,x])=>{
    const a=analyses[name]||{}, c=x.change_pct;
    const cc=c==null?'yellow':c>=0?'green':'red';
    const b=a.bias||'WAITING';
    return `<div class="card">
      <div class="card-head"><div class="label">${name}</div>${biasBadge(b)}</div>
      <div class="price">${fmt(x.ltp)}</div>
      <div class="change ${cc}">${c==null?'Waiting for live change':(c>=0?'+':'')+fmt(c)+'%'}</div>
      <div class="section">
        <div class="label">5M Market Bias</div>
        <div class="bias ${biasClass(b)}">${b}</div>
        <div class="conf">Confidence ${fmt(a.confidence||0)}%</div>
      </div>
      <div class="metrics">
        <div class="metric"><span>EMA 20</span><b>${fmt(a.ema20)}</b></div>
        <div class="metric"><span>EMA 50</span><b>${fmt(a.ema50)}</b></div>
        <div class="metric"><span>VWAP</span><b>${fmt(a.vwap)}</b></div>
        <div class="metric"><span>RSI 14</span><b>${fmt(a.rsi14)}</b></div>
      </div>
    </div>`;
  }).join('');
}
function renderOptions(data){
  let html='';
  Object.entries(data.underlyings||{}).forEach(([name,o])=>{
    const rows=o.data||[], a=o.analysis||{};
    html+=`<div class="option-wrap section">
      <div class="option-head">
        <div class="card-head"><div><b>${name}</b><div class="option-meta">${o.requested_expiry||'current'} • Spot ${fmt(o.spot)} • ATM ${fmt(o.atm_strike)}</div></div>${biasBadge(a.options_bias||'WAITING')}</div>
        <div class="option-stats">
          <div class="stat"><span>PCR (OI)</span><b>${fmt(a.pcr_oi)}</b></div>
          <div class="stat"><span>OI BIAS</span><b>${a.options_bias||'WAITING'}</b></div>
          <div class="stat"><span>CALL OI WALL</span><b>${fmt(a.oi_reference_resistance)}</b></div>
          <div class="stat"><span>PUT OI WALL</span><b>${fmt(a.oi_reference_support)}</b></div>
        </div>
        <div class="wall">Call OI Δ ${fmt(a.call_oi_change)} • Put OI Δ ${fmt(a.put_oi_change)} • Status ${o.status||'waiting'}</div>
        ${o.error?`<div class="error">${o.error}</div>`:''}
      </div>
      ${rows.length?`<div class="table-scroll"><table>
        <thead><tr><th>CALL OI</th><th>CALL LTP</th><th>STRIKE</th><th>PUT LTP</th><th>PUT OI</th></tr></thead>
        <tbody>${rows.map(r=>{
          const cc=r.call||{},pp=r.put||{};
          const atm=Number(r.strike)===Number(o.atm_strike);
          return `<tr class="${atm?'atm':''}">
            <td>${fmt(cc.oi)}</td><td>${fmt(cc.ltp)}</td><td>${fmt(r.strike)}</td>
            <td>${fmt(pp.ltp)}</td><td>${fmt(pp.oi)}</td>
          </tr>`;
        }).join('')}</tbody>
      </table></div>`:'<div class="empty">No option-chain data yet.</div>'}
    </div>`;
  });
  $('options').innerHTML=html||'<div class="option-wrap empty">Loading option chain…</div>';
}
function renderSignals(analyses, options){
  $('signals').innerHTML=Object.keys(analyses).map(name=>{
    const a=analyses[name]||{}, o=options?.[name]?.analysis||{};
    const b=a.bias||'WAITING';
    const structure=a.structure?.state||'—';
    return `<div class="engine-item">
      <b class="${biasClass(b)}">${name} • ${b}</b>
      <span>Technical score ${fmt(a.score)} • Confidence ${fmt(a.confidence||0)}%<br>
      Structure ${structure}<br>
      Options ${o.options_bias||'WAITING'} • PCR ${fmt(o.pcr_oi)}</span>
    </div>`;
  }).join('');
}
async function refresh(){
  try{
    const d=await (await fetch('/api/live',{cache:'no-store'})).json();
    $('status').textContent=(d.connection||'unknown').toUpperCase();
    $('dot').className='dot '+(d.connection==='connected'?'ok':'');
    $('clock').textContent=new Date(d.timestamp||Date.now()).toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
    renderCards(d.instruments||{},d.analysis||{});

    const opt=await (await fetch('/api/options-chain',{cache:'no-store'})).json();
    renderOptions(opt);
    renderSignals(d.analysis||{},opt.underlyings||{});
  }catch(e){
    $('status').textContent='API ERROR';
    $('dot').className='dot';
  }
}
refresh();
setInterval(refresh,5000);
</script>
</body>
</html>""")



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
