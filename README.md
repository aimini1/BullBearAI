# AI Market Radar V4 — Live Data Foundation

This is the starter package for the real-time dashboard.

## Important
Do NOT paste your Upstox token into chat, GitHub, or the browser frontend.

## Local setup
1. Install Python 3.10+.
2. Copy `.env.example` to `.env`.
3. Put your Upstox Analytics Token in `.env`.
4. Run:
   `pip install -r requirements.txt`
5. Start:
   `uvicorn main:app --host 127.0.0.1 --port 8000`
6. Open:
   `http://127.0.0.1:8000`

## Current V4
- Secure server-side token location
- FastAPI dashboard shell
- NIFTY 50 / BANK NIFTY / INDIA VIX instrument keys
- Live-data status endpoint
- Placeholder bias engine that refuses to invent data

## Next code module
Wire Upstox `MarketDataStreamerV3` into `main.py`, subscribe in `full` or `ltpc` mode, update `state`, then add:
FII/DII → OI/PCR → VIX → global cues → news → weighted bias engine → alerts.

Official Upstox V3 feed uses WebSocket + Protobuf and supports these index instrument keys.
