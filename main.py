import os
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
import xml.etree.ElementTree as ET

import requests
import upstox_client
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response

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

# Option-chain API is relatively heavy. Keep the last good chain on screen and
# refresh it on a slower cadence so a temporary 429/network hiccup does not
# turn a working chain into WAITING.
OPTION_REFRESH_SECONDS = 30
option_last_attempt = {name: None for name in OPTIONS_UNDERLYINGS}
option_last_good = {name: None for name in OPTIONS_UNDERLYINGS}

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
# Historical 5M candles are cached separately so the historical API is not
# hammered every 10 seconds. Live candles still refresh every cycle.
historical_cache = {name: [] for name in INSTRUMENTS}
historical_cache_at = {name: None for name in INSTRUMENTS}
analysis = {name: {} for name in INSTRUMENTS}
state["connection"] = "starting"
state["error"] = None
state["candle_status"] = "waiting"

# ODIN-style market-depth snapshot. Read-only for now; no orders are placed.
market_depth = {
    name: {"bids": [], "asks": [], "updated_at": None} for name in INSTRUMENTS
}


# Global macro / cross-market instruments supported by Upstox Global Instruments.
# Upstox documents GIFT NIFTY, global indices and indicators as GLOBAL_* instruments.
GLOBAL_MARKETS = {
    "GIFT NIFTY": "GLOBAL_INDEX|SGX NIFTY",
    "DOW JONES": "GLOBAL_INDEX|^DJI",
    "S&P 500": "GLOBAL_INDEX|^GSPC",
    "NASDAQ": "GLOBAL_INDEX|^IXIC",
    "FTSE 100": "GLOBAL_INDEX|^FTSE",
    "DAX": "GLOBAL_INDEX|^GDAXI",
    "NIKKEI 225": "GLOBAL_INDEX|^N225",
    "HANG SENG": "GLOBAL_INDEX|^HSI",
    "SHANGHAI": "GLOBAL_INDEX|000001.SS",
    "KOSPI": "GLOBAL_INDEX|^KS11",
    "USD/INR": "GLOBAL_INDICATOR|USDINR",
    "BRENT CRUDE": "GLOBAL_INDICATOR|BZUSD",
    "WTI CRUDE": "GLOBAL_INDICATOR|CLUSD",
    "US DOLLAR INDEX": "GLOBAL_INDICATOR|DX-Y.NYB",
}

global_state = {
    name: {
        "instrument_key": key,
        "ltp": None,
        "cp": None,
        "change_pct": None,
        "last_update": None,
        "status": "waiting",
    }
    for name, key in GLOBAL_MARKETS.items()
}
global_state_meta = {"status": "waiting", "error": None, "updated_at": None}

fii_dii_state = {
    "status": "waiting",
    "date": None,
    "fii": {"buy": None, "sell": None, "net": None},
    "dii": {"buy": None, "sell": None, "net": None},
    "error": None,
    "updated_at": None,
}

news_state = {
    "status": "waiting",
    "items": [],
    "updated_at": None,
    "error": None,
}

GLOBAL_QUOTE_URL = f"{UPSTOX}/v3/market-quote/ltp"
FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"
NEWS_RSS_URL = (
    "https://news.google.com/rss/search?"
    "q=(Nifty+OR+Sensex+OR+RBI+OR+FII+OR+crude+oil+OR+Fed+OR+inflation)+when:1d"
    "&hl=en-IN&gl=IN&ceid=IN:en"
)

def refresh_global_markets():
    """Refresh global indices, commodities and FX using Upstox Global Instruments."""
    headers = auth_headers()
    if not headers:
        global_state_meta.update(status="waiting", error="UPSTOX_ACCESS_TOKEN missing")
        return

    keys = list(GLOBAL_MARKETS.values())
    try:
        r = requests.get(
            GLOBAL_QUOTE_URL,
            headers=headers,
            params={"instrument_key": ",".join(keys)},
            timeout=20,
        )
        payload = r.json() if r.content else {}
        if not r.ok or payload.get("status") != "success":
            # Fall back to individual requests so one unavailable symbol
            # does not blank the entire macro page.
            for name, key in GLOBAL_MARKETS.items():
                try:
                    rr = requests.get(
                        GLOBAL_QUOTE_URL,
                        headers=headers,
                        params={"instrument_key": key},
                        timeout=7,
                    )
                    pp = rr.json() if rr.content else {}
                    if not rr.ok or pp.get("status") != "success":
                        continue
                    _update_global_from_payload(name, pp)
                except Exception:
                    continue
        else:
            data = payload.get("data") or {}
            for name, key in GLOBAL_MARKETS.items():
                item = data.get(key.replace("|", ":")) or data.get(key)
                if item:
                    _update_global_item(name, item)

        global_state_meta.update(
            status="connected",
            error=None,
            updated_at=now_ist(),
        )
    except Exception as exc:
        global_state_meta.update(status="error", error=str(exc), updated_at=now_ist())

def _update_global_from_payload(name, payload):
    data = payload.get("data") or {}
    key = GLOBAL_MARKETS[name]
    item = data.get(key.replace("|", ":")) or data.get(key)
    if item:
        _update_global_item(name, item)

def _update_global_item(name, item):
    ltp = item.get("last_price")
    cp = item.get("cp")
    if cp is None:
        cp = item.get("prev_close_price")
    pct = item.get("net_change")
    if pct is not None and cp not in (None, 0):
        pct = (float(pct) / float(cp)) * 100
    elif ltp is not None and cp not in (None, 0):
        pct = ((float(ltp) - float(cp)) / float(cp)) * 100
    global_state[name].update(
        ltp=float(ltp) if ltp is not None else None,
        cp=float(cp) if cp is not None else None,
        change_pct=round(float(pct), 2) if pct is not None else None,
        last_update=now_ist(),
        status="ready" if ltp is not None else "waiting",
    )

def refresh_fii_dii():
    """Fetch the latest published FII/DII cash-market activity from NSE."""
    try:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/131 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.nseindia.com/",
        })
        s.get("https://www.nseindia.com/", timeout=8)
        r = s.get(FII_DII_URL, timeout=12)
        payload = r.json() if r.content else []
        if not isinstance(payload, list):
            raise RuntimeError("Unexpected NSE FII/DII response")

        latest = None
        for row in payload:
            category = str(row.get("category") or "").upper()
            if category in ("FII/FPI", "DII"):
                latest = latest or {}
                latest[category] = row

        # Some NSE responses use "FII/FPI" and "DII"; parse the newest rows.
        fii = latest.get("FII/FPI") if latest else None
        dii = latest.get("DII") if latest else None

        def norm(row):
            if not row:
                return {"buy": None, "sell": None, "net": None}
            def num(*names):
                for n in names:
                    if row.get(n) not in (None, "", "-"):
                        try:
                            return float(str(row[n]).replace(",", ""))
                        except Exception:
                            pass
                return None
            buy = num("buyValue", "buy_value", "buy")
            sell = num("sellValue", "sell_value", "sell")
            net = num("netValue", "net_value", "net")
            if net is None and buy is not None and sell is not None:
                net = buy - sell
            return {"buy": buy, "sell": sell, "net": net}

        if fii or dii:
            fii_v, dii_v = norm(fii), norm(dii)
            fii_dii_state.update(
                status="ready",
                date=(fii or dii or {}).get("date"),
                fii=fii_v,
                dii=dii_v,
                error=None,
                updated_at=now_ist(),
            )
        else:
            raise RuntimeError("FII/DII rows not found")
    except Exception as exc:
        fii_dii_state.update(status="error", error=str(exc), updated_at=now_ist())

def refresh_news():
    """Fetch a lightweight market-news RSS feed; news remains a separate page."""
    try:
        r = requests.get(
            NEWS_RSS_URL,
            headers={"User-Agent": "BullBearAI/1.0"},
            timeout=12,
        )
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = []
        for node in root.findall(".//item")[:20]:
            title = (node.findtext("title") or "").strip()
            link = (node.findtext("link") or "").strip()
            pub = (node.findtext("pubDate") or "").strip()
            source = node.findtext("source")
            items.append({
                "title": title,
                "link": link,
                "published": pub,
                "source": (source or "Market News").strip(),
            })
        news_state.update(
            status="ready" if items else "waiting",
            items=items,
            error=None if items else "No headlines returned",
            updated_at=now_ist(),
        )
    except Exception as exc:
        news_state.update(status="error", error=str(exc), updated_at=now_ist())


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
    """Parse both LTPC and V3 FULL feed shapes from the Upstox SDK.

    Important: Upstox V3 index FULL data is under ff.indexFF, while
    non-index FULL data is commonly under ff.marketFF. Older examples and
    some SDK versions may expose fullFeed instead, so both are supported.
    """
    if not isinstance(feed, dict):
        # Some SDK versions may hand the callback a protobuf message.
        try:
            from google.protobuf.json_format import MessageToDict
            feed = MessageToDict(feed, preserving_proto_field_name=True)
        except Exception:
            return

    feeds = feed.get("feeds") or {}
    if not isinstance(feeds, dict):
        return

    for name, key in INSTRUMENTS.items():
        item = feeds.get(key) or feeds.get(key.replace("|", ":"))
        if not isinstance(item, dict):
            continue

        # V3 FULL: ff.indexFF for indices, ff.marketFF for tradable symbols.
        ff = item.get("ff") or item.get("fullFeed") or {}
        index_ff = ff.get("indexFF") or {}
        market_ff = ff.get("marketFF") or {}

        ltpc = item.get("ltpc") or market_ff.get("ltpc") or index_ff.get("ltpc")
        if not isinstance(ltpc, dict):
            continue

        try:
            ltp = float(ltpc["ltp"]) if ltpc.get("ltp") is not None else None
            cp = float(ltpc["cp"]) if ltpc.get("cp") is not None else None
        except (TypeError, ValueError):
            continue

        if ltp is None:
            continue
        pct = None if cp in (None, 0) else round(((ltp - cp) / cp) * 100, 2)

        # FULL feed also exposes traded volume in eFeedDetails for market data.
        details = market_ff.get("eFeedDetails") or index_ff.get("eFeedDetails") or {}
        volume = details.get("vtt") or details.get("volume")

        state[name].update(
            ltp=ltp, cp=cp, change_pct=pct,
            volume=volume, last_update=now_ist(),
        )

        # Market depth exists for tradable market instruments. Index feeds may
        # legitimately have no bid/ask order book, so keep an empty snapshot.
        level = market_ff.get("marketLevel") or ff.get("marketLevel") or {}
        quotes = level.get("bidAskQuote") or level.get("bidAsk") or []
        bids, asks = [], []
        if isinstance(quotes, list):
            for q in quotes:
                try:
                    bid_p = q.get("bidP", q.get("bid_price"))
                    ask_p = q.get("askP", q.get("ask_price"))
                    bid_q = q.get("bidQ", q.get("bid_qty", q.get("bidQuantity")))
                    ask_q = q.get("askQ", q.get("ask_qty", q.get("askQuantity")))
                    if bid_p is not None:
                        bids.append({"price": float(bid_p), "qty": float(bid_q or 0)})
                    if ask_p is not None:
                        asks.append({"price": float(ask_p), "qty": float(ask_q or 0)})
                except (TypeError, ValueError, AttributeError):
                    continue

        market_depth[name] = {
            "bids": bids[:5],
            "asks": asks[:5],
            "available": bool(bids or asks),
            "updated_at": now_ist(),
        }


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
            streamer.subscribe(list(INSTRUMENTS.values()), "full")

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
        option_chains[name]["error"] = "UPSTOX_ACCESS_TOKEN missing"
        return option_chains[name].get("data") or []
    headers = {**headers, "Accept": "application/json", "Content-Type": "application/json"}

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

        # This endpoint is current-day data. Keep it for the live/latest candles.
        return result[-300:]
    except Exception as exc:
        state["candle_status"] = f"candle_error: {exc}"
        return []


def fetch_historical_candles(key, interval, days=7):
    """Fetch recent historical candles so EMA/RSI can start immediately.
    Upstox V3 historical candle API supports multi-day minute history.
    """
    headers = auth_headers()
    if not headers:
        return []
    end_date = datetime.now(IST).date()
    start_date = end_date - timedelta(days=days)
    url = f"{UPSTOX}/v3/historical-candle/{quote(key, safe='')}/minutes/{interval}/{end_date.isoformat()}/{start_date.isoformat()}"
    try:
        response = requests.get(url, headers=headers, timeout=12)
        if not response.ok:
            state["candle_status"] = f"HIST HTTP {response.status_code}"
            return []
        rows = ((response.json() or {}).get("data") or {}).get("candles") or []
        result = []
        for row in rows:
            if len(row) < 6:
                continue
            try:
                o, h, l, c = map(float, row[1:5])
                vol = float(row[5]) if row[5] is not None else 0.0
            except (TypeError, ValueError):
                continue
            if any(v != v for v in (o, h, l, c)):
                continue
            result.append({
                "timestamp": row[0], "open": o, "high": h, "low": l,
                "close": c, "volume": vol,
                "oi": float(row[6]) if len(row) > 6 and row[6] is not None else None,
            })
        result.sort(key=lambda x: x["timestamp"])
        return result[-600:]
    except Exception as exc:
        state["candle_status"] = f"historical_error: {exc}"
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
    # EMA20/EMA50/RSI14 use combined recent historical + current-session 5M data.
    e20, e50 = ema(closes, 20), ema(closes, 50)
    rsi14 = rsi(closes, 14)
    # VWAP remains session-specific: previous sessions must not contaminate it.
    today_ist = datetime.now(IST).date()
    today_rows = []
    for row in rows:
        try:
            ts = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)
            if ts.astimezone(IST).date() == today_ist:
                today_rows.append(row)
        except (TypeError, ValueError):
            pass
    vw = vwap(today_rows)
    pvwap = None if vw is not None else price_vwap(today_rows)
    reference_vwap = vw if vw is not None else pvwap
    structure = market_structure(rows[-30:])

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


def _resolve_option_expiry(name, preferred=None):
    """Resolve a live expiry date using Upstox option-contract metadata.

    Relative expiry keywords are supported by Upstox, but resolving the actual
    date first gives us a reliable fallback when the relative keyword is
    temporarily unavailable or rolls over around expiry.
    """
    headers = auth_headers()
    if not headers:
        return preferred or OPTION_EXPIRIES.get(name, "current_week")

    key = OPTIONS_UNDERLYINGS[name]
    preferred = preferred or OPTION_EXPIRIES.get(name, "current_week")

    # First try the preferred relative expiry directly through the contracts API.
    try:
        r = requests.get(
            f"{UPSTOX}/v2/option/contract",
            headers=headers,
            params={"instrument_key": key, "expiry_date": preferred},
            timeout=10,
        )
        payload = r.json() if r.content else {}
        contracts = payload.get("data") or [] if r.ok else []
        dates = sorted({str(x.get("expiry")) for x in contracts if x.get("expiry")})
        if dates:
            return dates[0]
    except Exception:
        pass

    # Fallback: ask for all option contracts and choose the nearest future expiry.
    try:
        r = requests.get(
            f"{UPSTOX}/v2/option/contract",
            headers=headers,
            params={"instrument_key": key},
            timeout=10,
        )
        payload = r.json() if r.content else {}
        contracts = payload.get("data") or [] if r.ok else []
        dates = sorted({str(x.get("expiry")) for x in contracts if x.get("expiry")})
        if dates:
            today = datetime.now(IST).date().isoformat()
            future = [d for d in dates if d >= today]
            if future:
                return future[0]
            return dates[0]
    except Exception:
        pass

    return preferred


def _request_option_chain(name, expiry, headers):
    key = OPTIONS_UNDERLYINGS[name]
    response = requests.get(
        f"{UPSTOX}/v2/option/chain",
        headers=headers,
        params={"instrument_key": key, "expiry_date": expiry},
        timeout=12,
    )
    payload = response.json() if response.content else {}
    return response, payload


def fetch_option_chain(name, expiry=None):
    """Fetch a live Upstox option chain with an actual-expiry fallback."""
    headers = auth_headers()
    if not headers:
        option_chains[name]["error"] = "UPSTOX_ACCESS_TOKEN is missing"
        return option_chains[name].get("data") or []

    preferred = expiry or OPTION_EXPIRIES.get(name, "current_week")
    actual_expiry = _resolve_option_expiry(name, preferred)
    attempts = []
    for candidate in [actual_expiry, preferred, "current_month"]:
        if candidate and candidate not in attempts:
            attempts.append(candidate)

    last_error = None
    for candidate in attempts:
        try:
            response, payload = _request_option_chain(name, candidate, headers)
            if not response.ok or payload.get("status") != "success":
                last_error = payload.get("errors") or payload.get("message") or f"HTTP {response.status_code}"
                continue

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

                def oi_change(md):
                    oi, prev = md.get("oi"), md.get("prev_oi")
                    return (oi - prev) if oi is not None and prev is not None else None

                clean.append({
                    "expiry": row.get("expiry") or candidate,
                    "strike": strike,
                    "spot": row.get("underlying_spot_price"),
                    "pcr": row.get("pcr"),
                    "call": {
                        "instrument_key": call.get("instrument_key"),
                        "ltp": call_md.get("ltp"), "volume": call_md.get("volume"),
                        "oi": call_md.get("oi"), "prev_oi": call_md.get("prev_oi"),
                        "change_oi": oi_change(call_md),
                        "iv": call_g.get("iv", call_md.get("iv")),
                        "delta": call_g.get("delta"), "gamma": call_g.get("gamma"),
                        "theta": call_g.get("theta"), "vega": call_g.get("vega"),
                        "pop": call_g.get("pop"),
                    },
                    "put": {
                        "instrument_key": put.get("instrument_key"),
                        "ltp": put_md.get("ltp"), "volume": put_md.get("volume"),
                        "oi": put_md.get("oi"), "prev_oi": put_md.get("prev_oi"),
                        "change_oi": oi_change(put_md),
                        "iv": put_g.get("iv", put_md.get("iv")),
                        "delta": put_g.get("delta"), "gamma": put_g.get("gamma"),
                        "theta": put_g.get("theta"), "vega": put_g.get("vega"),
                        "pop": put_g.get("pop"),
                    },
                })

            if not clean:
                last_error = f"Upstox returned an empty option chain for expiry {candidate}"
                continue

            clean.sort(key=lambda x: x["strike"])
            spots = [x.get("spot") for x in clean if x.get("spot") is not None]
            spot = float(spots[0]) if spots else None
            atm_strike = min(clean, key=lambda x: abs(x["strike"] - spot))["strike"] if spot is not None else None

            option_chains[name].update(
                status="success", expiry=str(clean[0].get("expiry") or candidate),
                spot=spot, atm_strike=atm_strike, count=len(clean), data=clean,
                updated_at=now_ist(), error=None, last_error_at=None,
            )
            return clean

        except Exception as exc:
            last_error = str(exc)

    # Keep last good data rather than switching the UI to WAITING on a transient error.
    option_chains[name]["error"] = last_error or "Option-chain request failed"
    option_chains[name]["last_error_at"] = now_ist()
    return option_chains[name].get("data") or []


def option_setup_map(name):
    """Build a rule-based strike reference map from live technical + OI data.

    This is an analytical reference only. It does not place orders or claim that
    a strike is guaranteed to work. The UI labels these as candidates/references.
    """
    a = analysis.get(name) or {}
    opt = option_chain_analysis(name)
    rows = option_chains.get(name, {}).get("data") or []
    if a.get("status") != "ready" or opt.get("status") != "ready" or not rows:
        return {"status": "waiting", "bias": "WAITING", "reason": "Waiting for technical + option-chain data."}

    spot = a.get("price") or option_chains[name].get("spot")
    atm = option_chains[name].get("atm_strike")
    if spot is None or atm is None:
        return {"status": "waiting", "bias": "WAITING", "reason": "ATM strike is not available yet."}

    strikes = sorted({float(r.get("strike")) for r in rows if r.get("strike") is not None})
    if not strikes:
        return {"status": "waiting", "bias": "WAITING", "reason": "Strike data is not available yet."}

    atm = min(strikes, key=lambda x: abs(x - float(atm)))
    call_wall = opt.get("oi_reference_resistance")
    put_wall = opt.get("oi_reference_support")

    bias = a.get("bias", "NEUTRAL")
    # Require technical and options context to agree before showing a directional map.
    tech = a.get("technical_score", 0)
    opt_score = a.get("option_score", 0)
    final = a.get("final_score", 0)

    if bias == "BULLISH":
        direction = "BULLISH SETUP MAP"
        buy_strike = atm
        sell_strike = put_wall
        buy_label = "CE BUY CANDIDATE"
        sell_label = "PE SELL REFERENCE"
        logic = "Bullish technical bias + option confirmation; ATM CE is the directional reference, while Put-OI wall is the support reference."
    elif bias == "BEARISH":
        direction = "BEARISH SETUP MAP"
        buy_strike = atm
        sell_strike = call_wall
        buy_label = "PE BUY CANDIDATE"
        sell_label = "CE SELL REFERENCE"
        logic = "Bearish technical bias + option confirmation; ATM PE is the directional reference, while Call-OI wall is the resistance reference."
    else:
        direction = "NO DIRECTIONAL SETUP"
        buy_strike = None
        sell_strike = None
        buy_label = "WAIT"
        sell_label = "WAIT"
        logic = "Technical and option inputs are not aligned enough for a directional strike map."

    return {
        "status": "ready",
        "bias": bias,
        "direction": direction,
        "spot": round(float(spot), 2),
        "atm": atm,
        "buy_strike": buy_strike,
        "sell_strike": sell_strike,
        "buy_label": buy_label,
        "sell_label": sell_label,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "technical_score": tech,
        "option_score": opt_score,
        "final_score": final,
        "confidence": a.get("confidence", 0),
        "ema20": a.get("ema20"),
        "ema50": a.get("ema50"),
        "vwap": a.get("vwap"),
        "rsi14": a.get("rsi14"),
        "pcr": opt.get("pcr_oi"),
        "logic": logic,
        "risk_note": "Strike levels are rule-based references, not a trade recommendation. Confirm price action, liquidity, spread and risk before any order.",
    }


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
            "last_good_at": option_last_good.get(name),
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
        "last_good_at": option_last_good.get(name),
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
            now = datetime.now(IST)

            # Refresh option chains every 30s, not every 10s. This reduces API
            # pressure while still keeping the displayed OI/LTP data live.
            for option_name in OPTIONS_UNDERLYINGS:
                last_attempt = option_last_attempt.get(option_name)
                age = (now - last_attempt).total_seconds() if last_attempt else 10**9
                if age >= OPTION_REFRESH_SECONDS or not option_chains[option_name].get("data"):
                    option_last_attempt[option_name] = now
                    result = fetch_option_chain(
                        option_name,
                        OPTION_EXPIRIES.get(option_name, "current_week"),
                    )
                    if result:
                        option_last_good[option_name] = now

            for name, key in INSTRUMENTS.items():
                # 1M is used as the live/current-session source.
                one = fetch_candles(key, 1)

                # Historical 5M data is expensive and does not need a 10-second
                # refresh. Cache it for 5 minutes and keep the last good copy
                # if a temporary API/rate-limit error occurs.
                cache_at = historical_cache_at.get(name)
                cache_age = (now - cache_at).total_seconds() if cache_at else 10**9
                if not historical_cache[name] or cache_age >= 300:
                    hist5 = fetch_historical_candles(key, 5, days=7)
                    if hist5:
                        historical_cache[name] = hist5
                        historical_cache_at[name] = now
                else:
                    hist5 = historical_cache[name]

                # Current-day 5M candles are lightweight and refreshed every cycle.
                live5 = fetch_candles(key, 5)

                # Combine previous sessions + current session, de-duplicating by
                # timestamp. This gives EMA20/EMA50/RSI14 enough history immediately.
                merged = {r["timestamp"]: r for r in (hist5 or [])}
                for r in live5:
                    merged[r["timestamp"]] = r
                five = sorted(merged.values(), key=lambda x: x["timestamp"])[-600:]

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

        time.sleep(10)

def macro_refresh_loop():
    while True:
        refresh_global_markets()
        refresh_fii_dii()
        refresh_news()
        time.sleep(60)

@app.on_event("startup")
def startup():
    threading.Thread(target=start_websocket, daemon=True).start()
    threading.Thread(target=refresh_data, daemon=True).start()
    threading.Thread(target=macro_refresh_loop, daemon=True).start()


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
    snapshot["requested_expiry"] = option_chains[name].get("expiry") or OPTION_EXPIRIES.get(name, "current_week")
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
        snapshot["requested_expiry"] = option_chains[name].get("expiry") or OPTION_EXPIRIES.get(name, "current_week")
        result[name] = snapshot

    return {
        "timestamp": now_ist(),
        "underlyings": result,
        "analysis": {
            name: option_chain_analysis(name)
            for name in OPTIONS_UNDERLYINGS
        },
    }



@app.get("/api/global-market")
def global_market_api():
    return {
        "timestamp": now_ist(),
        "status": global_state_meta,
        "markets": global_state,
        "fii_dii": fii_dii_state,
    }

@app.get("/api/news")
def news_api():
    return {
        "timestamp": now_ist(),
        **news_state,
    }

@app.get("/api/live")
def live():
    setups = {name: option_setup_map(name) for name in OPTIONS_UNDERLYINGS}
    return {
        **market_data(),
        # The terminal chart reads the latest 5M candles directly from this
        # endpoint so it can render without an extra request.
        "candles": candles,
        "analysis": analysis,
        "setup_maps": setups,
        "market_depth": market_depth,
    }



@app.get("/manifest.webmanifest")
def pwa_manifest():
    return JSONResponse({
        "name": "BullBear AI",
        "short_name": "BullBear AI",
        "description": "Live Indian market analytics dashboard",
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "background_color": "#0b1220",
        "theme_color": "#0b1220",
        "orientation": "portrait-primary",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"}
        ]
    })

@app.get("/sw.js")
def pwa_service_worker():
    return Response(
        """const CACHE='bullbear-ai-v1';
self.addEventListener('install',event=>self.skipWaiting());
self.addEventListener('activate',event=>event.waitUntil(self.clients.claim()));
self.addEventListener('fetch',event=>{
  if(event.request.method!=='GET') return;
  const url=new URL(event.request.url);
  if(url.pathname.startsWith('/api/')) return;
  event.respondWith(fetch(event.request).catch(()=>caches.match(event.request)));
});""",
        media_type="application/javascript",
        headers={"Cache-Control":"no-cache"}
    )

@app.get("/icon-192.png")
def pwa_icon_192():
    return Response(PWA_ICON_PNG, media_type="image/png", headers={"Cache-Control":"public,max-age=86400"})

@app.get("/icon-512.png")
def pwa_icon_512():
    return Response(PWA_ICON_PNG, media_type="image/png", headers={"Cache-Control":"public,max-age=86400"})

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#07101d">
<link rel="manifest" href="/manifest.webmanifest">
<link rel="apple-touch-icon" href="/icon-192.png">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="BullBear AI">
<title>BullBear AI Terminal</title>
<style>
:root{--bg:#060b13;--panel:#0b1320;--panel2:#0e1827;--line:#1d2b3d;--text:#edf3fb;--muted:#8492a7;--green:#28d39a;--red:#ff5d70;--yellow:#e8bd52;--blue:#61a9ff;--shadow:0 8px 26px rgba(0,0,0,.26)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,Arial,sans-serif;font-size:12px}button{font:inherit}.app{min-height:100vh;padding-bottom:62px}.wrap{max-width:1400px;margin:auto;padding:9px}
.top{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px}.brand{display:flex;align-items:center;gap:8px}.logo{width:34px;height:34px;border-radius:9px;background:#101c2d;display:grid;place-items:center;font-size:18px}.brand b{font-size:15px}.sub{font-size:9px;color:var(--muted);margin-top:2px}.conn{display:flex;gap:6px;align-items:center;background:var(--panel);border:1px solid var(--line);border-radius:999px;padding:6px 9px;font-size:9px;font-weight:800}.dot{width:6px;height:6px;border-radius:50%;background:var(--yellow)}.dot.ok{background:var(--green)}
.tape{display:grid;grid-template-columns:repeat(3,1fr);gap:5px;margin-bottom:7px}.ticker{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:7px 8px;min-width:0}.ticker-top{display:flex;justify-content:space-between;gap:5px;color:var(--muted);font-size:9px;font-weight:800}.ticker-price{font-size:16px;font-weight:900;margin-top:3px}.ticker-change{font-size:9px;font-weight:900}.up{color:var(--green)}.down{color:var(--red)}.wait{color:var(--yellow)}
.toolbar{display:flex;gap:5px;overflow:auto;margin-bottom:7px}.btn{border:1px solid var(--line);background:#0d1725;color:#9daabd;border-radius:7px;padding:7px 10px;font-size:9px;font-weight:900;white-space:nowrap;cursor:pointer}.btn.active{background:#eaf1fa;color:#07101d;border-color:#eaf1fa}.grid{display:grid;grid-template-columns:300px minmax(0,1fr) 300px;gap:7px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:9px;box-shadow:var(--shadow);overflow:hidden}.ph{display:flex;justify-content:space-between;align-items:center;padding:8px 9px;background:#0d1725;border-bottom:1px solid var(--line)}.ph b{font-size:10px}.tag{font-size:8px;color:var(--muted);font-weight:800}.body{padding:8px}.row{display:grid;grid-template-columns:1fr 70px 70px;gap:5px;align-items:center;padding:7px 5px;border-bottom:1px solid #172436}.row:last-child{border-bottom:0}.row.head{font-size:8px;color:var(--muted);font-weight:800;padding-top:3px}.sym{font-weight:900}.ltp{text-align:right;font-weight:900}.pct{text-align:right;font-weight:900}.mini{font-size:8px;color:var(--muted)}
.depth{display:grid;grid-template-columns:1fr 1fr;gap:7px}.depth h4{margin:0 0 5px;font-size:8px;color:var(--muted)}.depthrow{display:flex;justify-content:space-between;padding:4px 5px;border-bottom:1px solid #172436;font-size:9px}.depthrow b{font-size:9px}.ask{color:var(--red)}.bid{color:var(--green)}
.quote{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;margin-bottom:7px}.qbox{background:#0d1725;border:1px solid var(--line);border-radius:7px;padding:7px}.qbox span{display:block;color:var(--muted);font-size:8px}.qbox b{display:block;margin-top:3px;font-size:11px}
.chart{height:190px;background:#07101a;border:1px solid var(--line);border-radius:7px;overflow:hidden;position:relative}.chart svg{width:100%;height:100%}.chartlabel{position:absolute;left:7px;top:6px;color:var(--muted);font-size:8px}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;margin-top:7px}.metric{background:#0d1725;border:1px solid var(--line);border-radius:7px;padding:7px}.metric span{display:block;color:var(--muted);font-size:8px}.metric b{display:block;margin-top:3px;font-size:10px}
.score{margin-top:7px;padding:8px;border:1px solid var(--line);border-radius:7px;background:#0d1725}.scoreline{display:flex;justify-content:space-between;align-items:center}.big{font-size:16px;font-weight:950}.reason{font-size:8px;color:var(--muted);line-height:1.5;margin-top:5px}
.ocwrap{overflow:auto}.oc{width:100%;border-collapse:collapse;min-width:620px;font-size:8px}.oc th{background:#111d2d;color:var(--muted);padding:6px;text-align:right}.oc th:nth-child(3),.oc td:nth-child(3){text-align:center}.oc td{padding:6px;border-top:1px solid #172436;text-align:right}.atm{background:#28230f!important;color:#f2d56d;font-weight:900}.ce{color:#8ed6ff}.pe{color:#ff9baa}.tabs2{display:flex;gap:4px;margin-bottom:6px}.tiny{border:1px solid var(--line);background:#0d1725;color:#9daabd;border-radius:6px;padding:5px 8px;font-size:8px;font-weight:800}.tiny.active{color:#fff;background:#17253a}
.side-stat{display:grid;grid-template-columns:1fr 1fr;gap:5px}.sidebox{background:#0d1725;border:1px solid var(--line);border-radius:7px;padding:7px}.sidebox span{font-size:8px;color:var(--muted)}.sidebox b{display:block;font-size:11px;margin-top:3px}.bar{height:5px;background:#172436;border-radius:99px;overflow:hidden;margin-top:6px}.bar i{display:block;height:100%;background:var(--blue);width:50%}
.bottom{position:fixed;z-index:30;bottom:7px;left:50%;transform:translateX(-50%);width:min(720px,calc(100% - 14px));background:rgba(8,14,24,.97);border:1px solid var(--line);border-radius:10px;padding:4px;display:grid;grid-template-columns:repeat(4,1fr);gap:3px}.bottom button{border:0;background:transparent;color:#8190a5;padding:7px;font-size:8px;font-weight:900;border-radius:7px}.bottom button.active{background:#eaf1fa;color:#07101d}
.page{display:none}.page.active{display:block}.note{color:var(--muted);font-size:8px;line-height:1.45}.macrogrid{display:grid;grid-template-columns:repeat(4,1fr);gap:5px}.news{padding:8px 0;border-bottom:1px solid var(--line)}.news a{color:#dfe8f4;text-decoration:none;font-weight:800}.news small{display:block;color:var(--muted);margin-top:3px;font-size:8px}
@media(max-width:1050px){.grid{grid-template-columns:240px minmax(0,1fr) 240px}.quote,.metrics{grid-template-columns:repeat(2,1fr)}}
@media(max-width:760px){.wrap{padding:6px}.tape{grid-template-columns:1fr 1fr}.ticker:last-child{grid-column:1/-1}.grid{grid-template-columns:1fr}.rightcol{display:none}.panel.center{order:1}.leftcol{order:2}.macrogrid{grid-template-columns:1fr 1fr}.bottom{width:calc(100% - 12px)}.chart{height:170px}}
</style></head>
<body><div class="app"><div class="wrap">
<div class="top"><div class="brand"><div class="logo">🐂</div><div><b>BullBear AI Terminal</b><div class="sub">ODIN-style market workstation • Read-only analytics</div></div></div><div class="conn"><i id="dot" class="dot"></i><span id="status">CONNECTING</span></div></div>
<div id="tape" class="tape"></div>
<div class="toolbar"><button class="btn active" data-page="terminal">TERMINAL</button><button class="btn" data-page="options">OPTION CHAIN</button><button class="btn" data-page="market">MARKET DATA</button><button class="btn" data-page="signals">SIGNALS</button><button class="btn" data-page="news">NEWS</button></div>
<section id="terminal" class="page active"><div class="grid">
<div class="leftcol"><div class="panel"><div class="ph"><b>MARKET WATCH</b><span class="tag">LIVE</span></div><div id="watch" class="body"></div></div>
<div class="panel" style="margin-top:7px"><div class="ph"><b>MARKET DEPTH</b><span id="depthSym" class="tag">NIFTY 50</span></div><div class="body"><div class="depth"><div><h4>BID QTY • PRICE</h4><div id="bids"></div></div><div><h4>PRICE • ASK QTY</h4><div id="asks"></div></div></div></div></div></div>
<div class="panel center"><div class="ph"><b id="centerTitle">NIFTY 50</b><span id="centerBias" class="tag">WAITING</span></div><div class="body"><div id="quote" class="quote"></div><div class="chart"><span class="chartlabel">5M PRICE ACTION</span><svg id="chartSvg" viewBox="0 0 800 190" preserveAspectRatio="none"><polyline id="chartLine" fill="none" stroke="#61a9ff" stroke-width="2" points=""/></svg></div><div id="metrics" class="metrics"></div><div id="score" class="score"></div></div></div>
<div class="rightcol"><div class="panel"><div class="ph"><b>OPTION RADAR</b><span class="tag">OI + PCR</span></div><div id="optionRadar" class="body"></div></div><div class="panel" style="margin-top:7px"><div class="ph"><b>QUICK MACRO</b><span class="tag">GLOBAL</span></div><div id="quickMacro" class="body"></div></div></div>
</div></section>
<section id="options" class="page"><div class="panel"><div class="ph"><b>OPTION CHAIN</b><span class="tag">ATM ±5</span></div><div id="optionPage" class="body">Loading…</div></div></section>
<section id="market" class="page"><div class="panel"><div class="ph"><b>MARKET DATA</b><span class="tag">GLOBAL + FII/DII</span></div><div id="marketPage" class="body">Loading…</div></div></section>
<section id="signals" class="page"><div class="panel"><div class="ph"><b>SIGNAL ENGINE</b><span class="tag">5M</span></div><div id="signalPage" class="body">Loading…</div></div></section>
<section id="news" class="page"><div class="panel"><div class="ph"><b>MARKET NEWS</b><span class="tag">LATEST</span></div><div id="newsPage" class="body">Loading…</div></div></section>
<div class="note" style="padding:7px 2px;text-align:center">Analytical dashboard only. Market depth and option-chain data are live snapshots; no order is placed by this version.</div>
</div></div>
<div class="bottom"><button class="active" data-page="terminal">⌂ TERMINAL</button><button data-page="options">⌁ OPTIONS</button><button data-page="market">◎ MARKET</button><button data-page="signals">⚡ SIGNALS</button></div>
<script>
const $=id=>document.getElementById(id);const fmt=v=>v==null?'—':Number(v).toLocaleString('en-IN',{maximumFractionDigits:2});
const cls=v=>v==='BULLISH'?'up':v==='BEARISH'?'down':'wait';const esc=s=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
let live={},options={},macro={},news={},selected='NIFTY 50';
function setPage(p){document.querySelectorAll('.page').forEach(x=>x.classList.toggle('active',x.id===p));document.querySelectorAll('[data-page]').forEach(x=>x.classList.toggle('active',x.dataset.page===p));window.scrollTo(0,0)}
document.querySelectorAll('[data-page]').forEach(x=>x.onclick=()=>setPage(x.dataset.page));
function renderTape(){const ins=live.instruments||{};$('tape').innerHTML=Object.entries(ins).map(([n,x])=>`<div class="ticker"><div class="ticker-top"><span>${esc(n)}</span><span>${x.last_update?new Date(x.last_update).toLocaleTimeString('en-IN'):''}</span></div><div class="ticker-price">${fmt(x.ltp)}</div><div class="ticker-change ${x.change_pct==null?'wait':x.change_pct>=0?'up':'down'}">${x.change_pct==null?'WAITING':(x.change_pct>=0?'+':'')+fmt(x.change_pct)+'%'}</div></div>`).join('')}
function renderWatch(){const a=live.analysis||{};const ins=live.instruments||{};$('watch').innerHTML=Object.keys(ins).map(n=>{const x=ins[n],z=a[n]||{};return `<div class="row" onclick="selectSymbol('${esc(n)}')"><div><div class="sym">${esc(n)}</div><div class="mini">${esc(z.bias||'WAITING')} • score ${z.final_score??0}</div></div><div class="ltp">${fmt(x.ltp)}</div><div class="pct ${x.change_pct==null?'wait':x.change_pct>=0?'up':'down'}">${x.change_pct==null?'—':(x.change_pct>=0?'+':'')+fmt(x.change_pct)+'%'}</div></div>`}).join('')}
function selectSymbol(n){selected=n;renderTerminal()}
function renderDepth(){const d=live.market_depth?.[selected]||{};$('depthSym').textContent=selected;if(!(d.bids||[]).length&&!(d.asks||[]).length){const msg=selected.includes('NIFTY')||selected==='INDIA VIX'?'Index feed: order-book depth is not supplied for this index. Option/futures depth can be added separately.':'Depth waiting…';$('bids').innerHTML=`<div class="mini">${msg}</div>`;$('asks').innerHTML=`<div class="mini">${msg}</div>`;return}$('bids').innerHTML=(d.bids||[]).map(x=>`<div class="depthrow bid"><b>${fmt(x.qty)}</b><span>${fmt(x.price)}</span></div>`).join('');$('asks').innerHTML=(d.asks||[]).map(x=>`<div class="depthrow ask"><span>${fmt(x.price)}</span><b>${fmt(x.qty)}</b></div>`).join('')}
function renderChart(){const rows=live.candles?.[selected]?.['5m']||[];const pts=rows.slice(-70);if(!pts.length){$('chartLine').setAttribute('points','');return}const vals=pts.map(x=>Number(x.close));const mn=Math.min(...vals),mx=Math.max(...vals),range=mx-mn||1;const points=vals.map((v,i)=>`${(i/(vals.length-1||1))*800},${180-((v-mn)/range)*155-10}`).join(' ');$('chartLine').setAttribute('points',points)}
function renderTerminal(){const a=live.analysis?.[selected]||{},x=live.instruments?.[selected]||{};const o=options.underlyings?.[selected]||{},oa=o.analysis||{};$('centerTitle').textContent=selected;$('centerBias').textContent=a.bias||'WAITING';$('centerBias').className='tag '+cls(a.bias);$('quote').innerHTML=[['LTP',x.ltp],['CHANGE',x.change_pct==null?null:(x.change_pct>=0?'+':'')+fmt(x.change_pct)+'%'],['OI WALL',oa.oi_reference_resistance],['PCR',oa.pcr_oi]].map(q=>`<div class="qbox"><span>${q[0]}</span><b>${q[1]==null?'—':typeof q[1]==='number'?fmt(q[1]):q[1]}</b></div>`).join('');$('metrics').innerHTML=[['EMA20',a.ema20],['EMA50',a.ema50],['VWAP',a.vwap],['RSI14',a.rsi14]].map(q=>`<div class="metric"><span>${q[0]}</span><b>${fmt(q[1])}</b></div>`).join('');$('score').innerHTML=`<div class="scoreline"><span class="big ${cls(a.bias)}">${a.bias||'WAITING'}</span><b>Final ${a.final_score??0} • ${a.confidence??0}%</b></div><div class="reason">${esc((a.reasons||[]).join(' • ')||'Waiting for analysis')}</div>`;renderChart();renderDepth();}
function renderOptionRadar(){const list=['NIFTY 50','BANK NIFTY'];$('optionRadar').innerHTML=list.map(n=>{const x=options.underlyings?.[n]||{},a=x.analysis||{};return `<div style="padding:5px 0 9px;border-bottom:1px solid var(--line)"><b>${n}</b><div class="side-stat" style="margin-top:6px"><div class="sidebox"><span>PCR</span><b>${fmt(a.pcr_oi)}</b></div><div class="sidebox"><span>BIAS</span><b>${esc(a.options_bias||'WAITING')}</b></div><div class="sidebox"><span>CALL WALL</span><b>${fmt(a.oi_reference_resistance)}</b></div><div class="sidebox"><span>PUT WALL</span><b>${fmt(a.oi_reference_support)}</b></div></div></div>`}).join('')}
function renderQuickMacro(){const m=macro.markets||{};const keys=['GIFT NIFTY','DOW JONES','NASDAQ','USD/INR','BRENT CRUDE','NIKKEI 225'];$('quickMacro').innerHTML=keys.map(k=>{const x=m[k]||{};return `<div class="row" style="grid-template-columns:1fr 75px 55px"><div class="sym">${k}</div><div class="ltp">${fmt(x.ltp)}</div><div class="pct ${x.change_pct==null?'wait':x.change_pct>=0?'up':'down'}">${x.change_pct==null?'—':(x.change_pct>=0?'+':'')+fmt(x.change_pct)+'%'}</div></div>`}).join('')}
function renderOptions(){let h='';Object.entries(options.underlyings||{}).forEach(([n,x])=>{const a=x.analysis||{},rows=x.data||[];h+=`<div style="margin-bottom:10px"><div class="quote"><div class="qbox"><span>${n}</span><b>${fmt(x.spot)}</b></div><div class="qbox"><span>ATM</span><b>${fmt(x.atm_strike)}</b></div><div class="qbox"><span>PCR</span><b>${fmt(a.pcr_oi)}</b></div><div class="qbox"><span>BIAS</span><b>${esc(a.options_bias||'WAITING')}</b></div></div>`;if(rows.length){h+=`<div class="ocwrap"><table class="oc"><thead><tr><th>CE OI</th><th>CE LTP</th><th>STRIKE</th><th>PE LTP</th><th>PE OI</th></tr></thead><tbody>`;rows.forEach(r=>{const c=r.call||{},p=r.put||{},at=Number(r.strike)===Number(x.atm_strike);h+=`<tr class="${at?'atm':''}"><td class="ce">${fmt(c.oi)}</td><td class="ce">${fmt(c.ltp)}</td><td><b>${fmt(r.strike)}</b></td><td class="pe">${fmt(p.ltp)}</td><td class="pe">${fmt(p.oi)}</td></tr>`});h+='</tbody></table></div>'}else h+=`<div class="note">${esc(x.error||'Option chain waiting…')}</div>`;h+='</div>'});$('optionPage').innerHTML=h||'No option data';}
function renderMarket(){const m=macro.markets||{},f=macro.fii_dii||{};const keys=['GIFT NIFTY','USD/INR','BRENT CRUDE','WTI CRUDE','DOW JONES','S&P 500','NASDAQ','FTSE 100','DAX','NIKKEI 225','HANG SENG','SHANGHAI'];let h='<div class="macrogrid">';keys.forEach(k=>{const x=m[k]||{};h+=`<div class="qbox"><span>${k}</span><b>${fmt(x.ltp)}</b><small class="${x.change_pct==null?'wait':x.change_pct>=0?'up':'down'}">${x.change_pct==null?'—':(x.change_pct>=0?'+':'')+fmt(x.change_pct)+'%'}</small></div>`});h+='</div><div style="height:7px"></div><div class="side-stat">'+[['FII BUY',f.fii?.buy],['FII SELL',f.fii?.sell],['FII NET',f.fii?.net],['DII NET',f.dii?.net]].map(q=>`<div class="sidebox"><span>${q[0]}</span><b>${q[1]==null?'—':Number(q[1]).toLocaleString('en-IN',{maximumFractionDigits:2})}</b></div>`).join('')+'</div>'; $('marketPage').innerHTML=h}
function renderSignals(){const a=live.analysis||{};$('signalPage').innerHTML=Object.entries(a).map(([n,x])=>`<div class="panel" style="margin-bottom:7px"><div class="ph"><b>${n}</b><span class="${cls(x.bias)}">${x.bias||'WAITING'}</span></div><div class="body"><div class="side-stat"><div class="sidebox"><span>TECH SCORE</span><b>${x.technical_score??0}</b></div><div class="sidebox"><span>OPTION SCORE</span><b>${x.option_score??0}</b></div><div class="sidebox"><span>FINAL</span><b>${x.final_score??0}</b></div><div class="sidebox"><span>CONFIDENCE</span><b>${x.confidence??0}%</b></div></div><div class="reason">${esc((x.reasons||[]).join(' • ')||'Waiting')}</div></div></div>`).join('')}
function renderNews(){const items=news.items||[];$('newsPage').innerHTML=items.length?items.slice(0,15).map(n=>`<div class="news"><a href="${esc(n.link)}" target="_blank" rel="noopener">${esc(n.title)}</a><small>${esc(n.source||'Market News')} • ${esc(n.published||'')}</small></div>`).join(''):'News waiting…'}
async function refresh(){try{live=await (await fetch('/api/live?ts='+Date.now(),{cache:'no-store'})).json();options=await (await fetch('/api/options-chain?ts='+Date.now(),{cache:'no-store'})).json();macro=await (await fetch('/api/global-market?ts='+Date.now(),{cache:'no-store'})).json();news=await (await fetch('/api/news?ts='+Date.now(),{cache:'no-store'})).json();$('status').textContent=(live.connection||'unknown').toUpperCase();$('dot').className='dot '+(live.connection==='connected'?'ok':'');renderTape();renderWatch();renderTerminal();renderOptionRadar();renderQuickMacro();renderOptions();renderMarket();renderSignals();renderNews()}catch(e){$('status').textContent='API ERROR';$('dot').className='dot'}}
refresh();setInterval(refresh,5000);
if('serviceWorker' in navigator)window.addEventListener('load',()=>navigator.serviceWorker.register('/sw.js').catch(()=>{}));
</script></body></html>""")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
