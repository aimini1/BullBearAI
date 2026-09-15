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


def combined_option_score(name):
    """
    Option-chain confirmation score for the descriptive signal engine.

    PCR is the primary option input. Net OI change is used as a small
    secondary confirmation. OI walls are displayed as reference levels and
    are not treated as guaranteed support/resistance.
    """
    opt = option_chain_analysis(name)
    if opt.get("status") != "ready":
        return 0, "WAITING", []

    pcr = opt.get("pcr_oi")
    if pcr is None:
        return 0, "WAITING", []

    score = 0
    reasons = []

    if pcr >= 1.20:
        score += 1
        reasons.append("PCR indicates PUT-OI heavy")
    elif pcr <= 0.80:
        score -= 1
        reasons.append("PCR indicates CALL-OI heavy")
    else:
        reasons.append("PCR is balanced")

    call_change = opt.get("call_oi_change")
    put_change = opt.get("put_oi_change")
    if call_change is not None and put_change is not None:
        if put_change > call_change and put_change > 0:
            score += 1
            reasons.append("put OI increase is stronger")
        elif call_change > put_change and call_change > 0:
            score -= 1
            reasons.append("call OI increase is stronger")

    if score > 0:
        label = "BULLISH OPTIONS"
    elif score < 0:
        label = "BEARISH OPTIONS"
    else:
        label = "BALANCED"

    return score, label, reasons


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
            "technical_score": 0,
            "option_score": 0,
            "final_score": 0,
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

    technical_score = 0
    technical_reasons = []

    # 1) Price vs EMA20: +1 / -1
    if e20 is not None:
        if current > e20:
            technical_score += 1
            technical_reasons.append("price above EMA20")
        elif current < e20:
            technical_score -= 1
            technical_reasons.append("price below EMA20")

    # 2) EMA20 vs EMA50: +2 / -2
    if e20 is not None and e50 is not None:
        if e20 > e50:
            technical_score += 2
            technical_reasons.append("EMA20 above EMA50")
        elif e20 < e50:
            technical_score -= 2
            technical_reasons.append("EMA20 below EMA50")

    # 3) Price vs VWAP: +1 / -1
    if reference_vwap is not None:
        if current > reference_vwap:
            technical_score += 1
            technical_reasons.append("price above VWAP reference")
        elif current < reference_vwap:
            technical_score -= 1
            technical_reasons.append("price below VWAP reference")

    # 4) RSI14: +1 / -1 only when momentum is clearly directional
    if rsi14 is not None:
        if rsi14 >= 55:
            technical_score += 1
            technical_reasons.append("RSI momentum positive")
        elif rsi14 <= 45:
            technical_score -= 1
            technical_reasons.append("RSI momentum negative")

    # 5) Market structure: +2 / -2
    if structure["state"] == "BULLISH_STRUCTURE":
        technical_score += 2
        technical_reasons.append("higher-high/higher-low structure")
    elif structure["state"] == "BEARISH_STRUCTURE":
        technical_score -= 2
        technical_reasons.append("lower-high/lower-low structure")

    option_score, option_bias, option_reasons = combined_option_score(name)
    final_score = technical_score + option_score
    reasons = technical_reasons + option_reasons

    # Technical engine has max 7 points; option confirmation max 2 points.
    # Keep the final decision descriptive rather than presenting certainty.
    if final_score >= 4:
        bias = "BULLISH"
    elif final_score <= -4:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    confidence = min(100, round(abs(final_score) / 9 * 100))

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
        "technical_score": technical_score,
        "option_score": option_score,
        "final_score": final_score,
        "option_bias": option_bias,
        "option_analysis": option_chain_analysis(name),
        "score": final_score,
        "bias": bias,
        "confidence": confidence,
        "reasons": reasons,
        "technical_reasons": technical_reasons,
        "option_reasons": option_reasons,
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

            # Refresh option-chain data first so the combined bias engine uses
            # the newest PCR/OI snapshot during the same refresh cycle.
            for option_name in OPTIONS_UNDERLYINGS:
                fetch_option_chain(option_name, OPTION_EXPIRIES.get(option_name, "current_week"))

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


@app.get("/api/combined-bias")
def combined_bias():
    """
    Return the current technical + option-chain combined descriptive bias.
    """
    result = {}
    for name in INSTRUMENTS:
        a = analysis.get(name) or {}
        opt = option_chain_analysis(name)
        result[name] = {
            "technical_bias": a.get("bias", "WAITING"),
            "technical_score": a.get("technical_score", 0),
            "option_bias": opt.get("options_bias", "WAITING"),
            "option_score": a.get("option_score", 0),
            "final_bias": a.get("bias", "WAITING"),
            "final_score": a.get("final_score", a.get("score", 0)),
            "confidence": a.get("confidence", 0),
            "ema20": a.get("ema20"),
            "ema50": a.get("ema50"),
            "vwap": a.get("vwap"),
            "rsi14": a.get("rsi14"),
            "pcr_oi": opt.get("pcr_oi"),
            "call_oi_wall": opt.get("oi_reference_resistance"),
            "put_oi_wall": opt.get("oi_reference_support"),
            "reasons": a.get("reasons", []),
            "note": "Combined descriptive analysis only; not a trading recommendation.",
        }
    return {"timestamp": now_ist(), "data": result}


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
    return HTMLResponse(r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0b1220">
<title>BullBear AI</title>
<style>
:root{--bg:#f5f7fb;--card:#fff;--ink:#111827;--muted:#6b7280;--line:#e7ebf2;--nav:#0b1220;--bull:#11845b;--bear:#c43d3d;--wait:#a26d00;--soft:#f1f4f8;--shadow:0 8px 24px rgba(16,24,40,.06)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,Arial,sans-serif}button{font:inherit}
.app{min-height:100vh;padding-bottom:78px}.wrap{max-width:1120px;margin:auto;padding:14px 14px 24px}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px}.brand{display:flex;align-items:center;gap:10px}
.logo{width:42px;height:42px;border-radius:13px;background:var(--nav);display:grid;place-items:center;color:#fff;font-size:21px}.brand h1{font-size:20px;margin:0}.sub{font-size:11px;color:var(--muted);margin-top:2px}
.status{display:flex;align-items:center;gap:7px;background:var(--card);border:1px solid var(--line);border-radius:999px;padding:8px 11px;font-size:11px;font-weight:700}.dot{width:7px;height:7px;border-radius:50%;background:var(--wait)}.dot.ok{background:var(--bull)}
.page{display:none}.page.active{display:block}.hero{background:linear-gradient(135deg,#0b1220,#18253b);color:#fff;border-radius:20px;padding:18px;box-shadow:var(--shadow);margin-bottom:13px}
.hero-row{display:flex;justify-content:space-between;align-items:end;gap:12px}.hero-title{font-size:12px;opacity:.7;font-weight:700;letter-spacing:.08em}.hero-big{font-size:28px;font-weight:850;margin-top:5px}.hero-note{font-size:11px;opacity:.7;text-align:right}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.card{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:15px;box-shadow:var(--shadow);margin-bottom:12px}
.card-head{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:11px}.kicker{font-size:11px;color:var(--muted);font-weight:800;letter-spacing:.06em}.badge{font-size:10px;font-weight:800;padding:5px 8px;border-radius:999px;background:var(--soft)}
.price{font-size:26px;font-weight:850;margin:5px 0}.change{font-size:12px;font-weight:800}.bull{color:var(--bull)}.bear{color:var(--bear)}.wait{color:var(--wait)}
.bias{font-size:19px;font-weight:900;margin:10px 0 2px}.confidence{font-size:11px;color:var(--muted)}.metrics{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-top:12px}
.metric{background:#f7f8fa;border-radius:11px;padding:9px}.metric span{display:block;color:var(--muted);font-size:10px}.metric b{display:block;margin-top:3px;font-size:13px}
.signal{border-top:1px solid var(--line);padding:11px 0}.signal:first-child{border-top:0;padding-top:0}.signal-title{font-size:14px;font-weight:800}.signal-meta{font-size:11px;color:var(--muted);margin-top:4px}
.tabs{display:flex;gap:7px;overflow:auto;margin:0 0 13px;padding-bottom:2px}.tab{white-space:nowrap;border:1px solid var(--line);background:#fff;color:#596273;border-radius:12px;padding:9px 13px;font-size:11px;font-weight:800;cursor:pointer}.tab.active{background:var(--nav);color:#fff;border-color:var(--nav)}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:13px}.oc{width:100%;border-collapse:collapse;min-width:610px;font-size:11px}.oc th{background:#f7f8fa;color:var(--muted);font-size:10px;padding:9px 6px;text-align:right}.oc th:nth-child(3),.oc td:nth-child(3){text-align:center}.oc td{padding:8px 6px;border-top:1px solid #f0f2f5;text-align:right}.atm{background:#fff4c9!important;font-weight:850}
.stat-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.stat{background:#f7f8fa;border-radius:12px;padding:10px}.stat span{font-size:10px;color:var(--muted)}.stat b{display:block;font-size:14px;margin-top:4px}
.bottom-nav{position:fixed;z-index:20;left:50%;bottom:10px;transform:translateX(-50%);width:min(560px,calc(100% - 20px));background:rgba(11,18,32,.96);border-radius:18px;padding:7px;display:grid;grid-template-columns:repeat(4,1fr);gap:4px;box-shadow:0 10px 35px rgba(0,0,0,.2)}
.navbtn{border:0;background:transparent;color:#9ba6b7;border-radius:13px;padding:8px 4px;font-size:10px;font-weight:800;cursor:pointer}.navbtn.active{background:#fff;color:#111827}.navicon{font-size:16px;display:block;margin-bottom:2px}
.footer-note{font-size:10px;color:var(--muted);text-align:center;margin:12px 0 0}
@media(max-width:760px){.grid{grid-template-columns:1fr}.stat-grid{grid-template-columns:1fr 1fr}.wrap{padding:11px 11px 22px}.hero-big{font-size:25px}.topbar{align-items:flex-start}}
</style>
</head>
<body>
<div class="app"><div class="wrap">
  <div class="topbar">
    <div class="brand"><div class="logo">🐂</div><div><h1>BullBear AI</h1><div class="sub">Live Indian Market Radar</div></div></div>
    <div class="status"><span id="dot" class="dot"></span><span id="status">CONNECTING</span></div>
  </div>

  <div class="tabs">
    <button class="tab active" data-page="home">Overview</button>
    <button class="tab" data-page="options">Option Chain</button>
    <button class="tab" data-page="technical">Technical</button>
    <button class="tab" data-page="signals">Signals</button>
  </div>

  <section id="home" class="page active">
    <div class="hero"><div class="hero-row">
      <div><div class="hero-title">MARKET RADAR</div><div id="heroBias" class="hero-big wait">WAITING</div></div>
      <div class="hero-note">5M ENGINE<br><span id="heroTime">—</span></div>
    </div></div>
    <div id="marketCards" class="grid"></div>
    <div class="card"><div class="card-head"><div class="kicker">LIVE SIGNAL ENGINE</div><div class="badge">TECH + OPTIONS</div></div><div id="homeSignals">Loading…</div></div>
  </section>

  <section id="options" class="page">
    <div class="card"><div class="card-head"><div><div class="kicker">OPTION CHAIN</div><div style="font-weight:800;margin-top:3px">Live analysis</div></div><div class="badge">CURRENT</div></div><div id="optionContent">Loading option chain…</div></div>
  </section>

  <section id="technical" class="page"><div id="techContent"></div></section>

  <section id="signals" class="page">
    <div class="card"><div class="card-head"><div class="kicker">SIGNAL ENGINE</div><div class="badge">DESCRIPTIVE</div></div><div id="signalContent">Loading…</div></div>
    <div class="card"><div class="card-head"><div class="kicker">ENGINE STATUS</div></div><div id="engineStatus" class="signal-meta">Waiting…</div></div>
  </section>

  <div class="footer-note">Analytical dashboard only • Not a trading recommendation</div>
</div></div>

<div class="bottom-nav">
  <button class="navbtn active" data-page="home"><span class="navicon">⌂</span>Overview</button>
  <button class="navbtn" data-page="options"><span class="navicon">⌁</span>Options</button>
  <button class="navbtn" data-page="technical"><span class="navicon">◫</span>Technical</button>
  <button class="navbtn" data-page="signals"><span class="navicon">⚡</span>Signals</button>
</div>

<script>
const $=id=>document.getElementById(id);
function fmt(v){return v==null?'—':Number(v).toLocaleString('en-IN',{maximumFractionDigits:2})}
function clsBias(v){return v==='BULLISH'?'bull':v==='BEARISH'?'bear':'wait'}
function esc(s){return String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function setPage(p){document.querySelectorAll('.page').forEach(x=>x.classList.toggle('active',x.id===p));document.querySelectorAll('[data-page]').forEach(x=>x.classList.toggle('active',x.dataset.page===p));window.scrollTo({top:0,behavior:'smooth'})}
document.querySelectorAll('[data-page]').forEach(x=>x.addEventListener('click',()=>setPage(x.dataset.page)));

function renderMarkets(d){
  const cards=Object.entries(d.instruments||{}).map(([name,x])=>{
    const a=d.analysis?.[name]||{},c=x.change_pct,bias=a.bias||'WAITING';
    return `<div class="card"><div class="card-head"><div class="kicker">${esc(name)}</div><div class="badge">${a.timeframe||'5M'}</div></div>
      <div class="price">${fmt(x.ltp)}</div><div class="change ${c==null?'wait':c>=0?'bull':'bear'}">${c==null?'Waiting':(c>=0?'+':'')+fmt(c)+'%'}</div>
      <div class="bias ${clsBias(bias)}">${bias}</div><div class="confidence">Confidence: ${a.confidence??0}% • Final score: ${a.final_score??0}</div>
      <div class="metrics"><div class="metric"><span>EMA20</span><b>${fmt(a.ema20)}</b></div><div class="metric"><span>EMA50</span><b>${fmt(a.ema50)}</b></div>
      <div class="metric"><span>VWAP</span><b>${fmt(a.vwap)}</b></div><div class="metric"><span>RSI14</span><b>${fmt(a.rsi14)}</b></div></div></div>`;
  }).join('');
  $('marketCards').innerHTML=cards||'<div class="card">No market data yet.</div>';
  const ready=Object.values(d.analysis||{}).filter(a=>a.status==='ready');
  const lead=ready.filter(a=>a.bias!=='NEUTRAL').sort((a,b)=>Math.abs(b.final_score||0)-Math.abs(a.final_score||0))[0];
  $('heroBias').textContent=lead?.bias||'WAITING';$('heroBias').className='hero-big '+clsBias(lead?.bias);
  $('heroTime').textContent=new Date().toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit'});
  $('homeSignals').innerHTML=Object.entries(d.analysis||{}).map(([name,a])=>`<div class="signal"><div class="signal-title">${esc(name)} • <span class="${clsBias(a.bias)}">${a.bias||'WAITING'}</span></div>
    <div class="signal-meta">Technical ${a.technical_score??0} • Options ${a.option_score??0} • Final ${a.final_score??0} • Confidence ${a.confidence??0}%</div></div>`).join('');
}

function renderTechnical(d){
  $('techContent').innerHTML=Object.entries(d.analysis||{}).map(([name,a])=>`<div class="card">
    <div class="card-head"><div class="kicker">${esc(name)} • 5M TECHNICAL</div><div class="badge">${a.status||'waiting'}</div></div>
    <div class="stat-grid"><div class="stat"><span>PRICE</span><b>${fmt(a.price)}</b></div><div class="stat"><span>EMA20</span><b>${fmt(a.ema20)}</b></div>
      <div class="stat"><span>EMA50</span><b>${fmt(a.ema50)}</b></div><div class="stat"><span>RSI14</span><b>${fmt(a.rsi14)}</b></div></div>
    <div class="stat-grid" style="margin-top:8px"><div class="stat"><span>VWAP</span><b>${fmt(a.vwap)}</b></div><div class="stat"><span>STRUCTURE</span><b>${esc(a.structure?.state||'—')}</b></div>
      <div class="stat"><span>TECH SCORE</span><b>${a.technical_score??0}</b></div><div class="stat"><span>5M CANDLES</span><b>${a.candles??0}</b></div></div>
    <div class="signal-meta" style="margin-top:11px"><b>Reasons:</b> ${esc((a.technical_reasons||[]).join(' • ')||'Waiting for enough candles')}</div>
  </div>`).join('');
}

function renderOptions(o){
  let html='';
  Object.entries(o.underlyings||{}).forEach(([name,x])=>{
    const a=x.analysis||{},rows=x.data||[];
    html+=`<div class="card"><div class="card-head"><div><div class="kicker">${esc(name)}</div><div class="signal-meta">${esc(x.requested_expiry||x.expiry||'current')} • Spot ${fmt(x.spot)} • ATM ${fmt(x.atm_strike)}</div></div><div class="badge">${esc(x.status||'waiting')}</div></div>`;
    if(a.status==='ready') html+=`<div class="stat-grid"><div class="stat"><span>PCR (OI)</span><b>${fmt(a.pcr_oi)}</b></div><div class="stat"><span>OI BIAS</span><b>${esc(a.options_bias)}</b></div>
      <div class="stat"><span>CALL OI WALL</span><b>${fmt(a.oi_reference_resistance)}</b></div><div class="stat"><span>PUT OI WALL</span><b>${fmt(a.oi_reference_support)}</b></div></div>
      <div class="signal-meta" style="margin:9px 0">Call OI Δ ${fmt(a.call_oi_change)} • Put OI Δ ${fmt(a.put_oi_change)}</div>`;
    if(rows.length){html+=`<div class="table-wrap"><table class="oc"><thead><tr><th>CALL OI</th><th>CALL LTP</th><th>STRIKE</th><th>PUT LTP</th><th>PUT OI</th></tr></thead><tbody>`;
      rows.forEach(r=>{const c=r.call||{},p=r.put||{},at=Number(r.strike)===Number(x.atm_strike);html+=`<tr class="${at?'atm':''}"><td>${fmt(c.oi)}</td><td>${fmt(c.ltp)}</td><td>${fmt(r.strike)}</td><td>${fmt(p.ltp)}</td><td>${fmt(p.oi)}</td></tr>`});
      html+='</tbody></table></div>'}else html+=`<div class="signal-meta">${esc(x.error||'Waiting for option-chain data…')}</div>`;
    html+='</div>';
  });
  $('optionContent').innerHTML=html||'<div class="card">No option-chain data yet.</div>';
}

function renderSignals(d){
  $('signalContent').innerHTML=Object.entries(d.analysis||{}).map(([name,a])=>`<div class="signal"><div class="signal-title">${esc(name)} • <span class="${clsBias(a.bias)}">${a.bias||'WAITING'}</span></div>
    <div class="signal-meta">Technical score <b>${a.technical_score??0}</b> • Option score <b>${a.option_score??0}</b> • Final score <b>${a.final_score??0}</b> • Confidence <b>${a.confidence??0}%</b></div>
    <div class="signal-meta" style="margin-top:7px">${esc((a.reasons||[]).join(' • ')||'Waiting for analysis')}</div></div>`).join('');
  const ready=Object.values(d.analysis||{}).filter(a=>a.status==='ready'),ema50=ready.filter(a=>a.ema50_ready).length,opts=ready.filter(a=>a.option_bias&&a.option_bias!=='WAITING').length;
  $('engineStatus').textContent=`LIVE 5M ONLY • EMA20/50 • VWAP • RSI14 • Market Structure • EMA50 ready ${ema50}/${ready.length} • Options ready ${opts}/${ready.length}`;
}

async function refresh(){
 try{
  const d=await (await fetch('/api/live?ts='+Date.now(),{cache:'no-store'})).json();
  $('status').textContent=(d.connection||'unknown').toUpperCase();$('dot').className='dot '+(d.connection==='connected'?'ok':'');
  renderMarkets(d);renderTechnical(d);renderSignals(d);
  const o=await (await fetch('/api/options-chain?ts='+Date.now(),{cache:'no-store'})).json();renderOptions(o);
 }catch(e){$('status').textContent='API ERROR';$('dot').className='dot'}
}
refresh();setInterval(refresh,5000);
</script>
</body></html>""")



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
