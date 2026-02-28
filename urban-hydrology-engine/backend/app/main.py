"""Urban Hydrology Engine — FastAPI entry point."""

import asyncio
import os
from datetime import datetime

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.websockets import WebSocket, WebSocketDisconnect
from sqlalchemy import text

from app.db import init_db, engine, async_session
from app.api.ingest import router as ingest_router, ingest_rain_internal
from app.api.map_state import router as map_state_router, run_cycle_internal
from app.api.ward_detail import router as ward_detail_router
from app.services.weather import fetch_delhi_rainfall
from app.services.forecast import fetch_delhi_forecast
from app.ws import manager as ws_manager

load_dotenv()

app = FastAPI(title="Urban Hydrology Engine")
app.include_router(ingest_router)
app.include_router(map_state_router)
app.include_router(ward_detail_router)

# Resolve frontend directory — works in Docker and local dev
_FRONTEND_DIR = os.getenv("FRONTEND_DIR", "/app/frontend")
app.mount("/static", StaticFiles(directory=_FRONTEND_DIR), name="static")

# Shared httpx client for tile proxy
_tile_client: httpx.AsyncClient | None = None
TILESERV_ORIGIN = os.getenv("TILESERV_URL", "http://tileserv:7800")

# Scheduler
scheduler = AsyncIOScheduler()


# ---------------------------------------------------------------------------
# Weather polling job
# ---------------------------------------------------------------------------

async def weather_poll_job():
    """Fetch live rainfall from OWM; ingest + run cycle if raining."""
    try:
        rain_events = await fetch_delhi_rainfall()
        if not rain_events:
            print(f"[{datetime.now().isoformat()}] Poll: no precipitation")
            return

        async with engine.connect() as conn:
            for event in rain_events:
                result = await ingest_rain_internal(event, conn)
                print(f"[{datetime.now().isoformat()}] Poll: rain ingested — "
                      f"{result['hotspots_in_polygon']} hotspots hit")
            await conn.commit()

        async with engine.connect() as conn:
            cycle = await run_cycle_internal(conn)
            print(f"[{datetime.now().isoformat()}] Poll: cycle triggered — "
                  f"{cycle['wards_triggered']} wards dispatched")

            # Broadcast to WebSocket clients
            await ws_manager.broadcast({
                "type": "cycle",
                "wards_triggered": cycle["wards_triggered"],
                "safe_wards": cycle["safe_wards"],
            })

    except Exception as exc:
        print(f"[{datetime.now().isoformat()}] Poll error: {exc}")


# ---------------------------------------------------------------------------
# Startup / Shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    """Init DB, start tile proxy client, start weather scheduler."""
    global _tile_client
    _tile_client = httpx.AsyncClient(base_url=TILESERV_ORIGIN, timeout=10)

    max_retries = 15
    for attempt in range(1, max_retries + 1):
        try:
            await init_db()
            print(f"DB initialised on attempt {attempt}")
            break
        except Exception as exc:
            print(f"DB not ready (attempt {attempt}/{max_retries}): {exc}")
            if attempt == max_retries:
                raise
            await asyncio.sleep(2)

    # Auto-seed if database is empty (first deploy on Render etc.)
    try:
        from app.auto_seed import auto_seed_if_empty
        await asyncio.to_thread(auto_seed_if_empty)
    except Exception as exc:
        print(f"Auto-seed check failed (non-fatal): {exc}")

    # Start weather polling
    interval = int(os.getenv("RAIN_POLL_INTERVAL_SECONDS", "600"))
    scheduler.add_job(weather_poll_job, "interval", seconds=interval,
                      id="weather_poll", replace_existing=True)
    scheduler.start()
    print(f"Weather polling started — every {interval}s")

    # Trigger initial weather fetch so status isn't stuck at 'unknown'
    try:
        await weather_poll_job()
        print("Initial weather fetch completed")
    except Exception as exc:
        print(f"Initial weather fetch failed (non-fatal): {exc}")


@app.on_event("shutdown")
async def on_shutdown():
    """Clean up scheduler and httpx client."""
    scheduler.shutdown(wait=False)
    if _tile_client:
        await _tile_client.aclose()

@app.get("/", include_in_schema=False)
async def index():
    """Serve the war room dashboard."""
    return FileResponse(os.path.join(_FRONTEND_DIR, "index.html"))


@app.get("/health")
async def health():
    """Ping the database and report status."""
    try:
        async with async_session() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected"}
    except Exception:
        return {"status": "error", "db": "unreachable"}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """Real-time push: cycle events, weather updates."""
    await ws_manager.connect(ws)
    try:
        while True:
            # Keep connection alive; client can send pings
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)


# ---------------------------------------------------------------------------
# 6-Hour Forecast — Open-Meteo predictive risk
# ---------------------------------------------------------------------------

@app.get("/forecast/6hour")
async def forecast_6hour():
    """Return 6-hour rainfall forecast + risk level for Delhi."""
    try:
        data = await fetch_delhi_forecast()
        return data
    except Exception as exc:
        return {"error": str(exc), "risk_level": "NONE", "hours": []}


# ---------------------------------------------------------------------------
# Tile proxy — forwards to pg_tileserv (same origin, no CORS issues)
# ---------------------------------------------------------------------------

@app.get("/tiles/{layer}/{z}/{x}/{y}.pbf")
async def tile_proxy(layer: str, z: int, x: int, y: int):
    """Reverse-proxy a single MVT tile from pg_tileserv."""
    upstream = f"/public.{layer}/{z}/{x}/{y}.pbf"
    try:
        resp = await _tile_client.get(upstream)
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type="application/vnd.mapbox-vector-tile",
            headers={"Cache-Control": "no-cache"},
        )
    except httpx.RequestError:
        return Response(content=b"", status_code=502)
