"""Urban Hydrology Engine — FastAPI entry point."""

import asyncio
import os
from datetime import datetime, timezone, timedelta

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.websockets import WebSocket, WebSocketDisconnect
from sqlalchemy import text

from app.db import init_db, engine, async_session
from app.api.ingest import router as ingest_router, ingest_rain_internal
from app.api.map_state import router as map_state_router, run_cycle_internal, load_hotspot_cache
from app.api.ward_detail import router as ward_detail_router
from app.api.readiness import router as readiness_router
from app.api.backtest import router as backtest_router
from app.api.floodline import router as floodline_router
from app.api.export    import router as export_router
from app.services.weather import fetch_delhi_rainfall
from app.services.forecast import fetch_delhi_forecast
from app.ws import manager as ws_manager

load_dotenv()

app = FastAPI(title="Urban Hydrology Engine")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
app.include_router(ingest_router)
app.include_router(map_state_router)
app.include_router(ward_detail_router)
app.include_router(readiness_router)
app.include_router(backtest_router)
app.include_router(floodline_router)
app.include_router(export_router)

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

            # Also broadcast fresh SSI
            try:
                from app.services.ssi import compute_ssi
                async with engine.connect() as ssi_conn:
                    ssi = await compute_ssi(ssi_conn)
                await ws_manager.broadcast({
                    "type":   "ssi_update",
                    "ssi":    ssi["ssi"],
                    "level":  ssi["level"],
                    "colour": ssi["colour"],
                })
            except Exception:
                pass

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

    max_retries = 30
    for attempt in range(1, max_retries + 1):
        try:
            await init_db()
            print(f"DB initialised on attempt {attempt}")
            break
        except Exception as exc:
            print(f"DB not ready (attempt {attempt}/{max_retries}): {exc}")
            if attempt == max_retries:
                print(
                    "WARNING: DB unreachable after all retries — starting anyway. "
                    "Set DATABASE_URL in the Render dashboard and redeploy."
                )
                break
            await asyncio.sleep(3)

    # Pre-load hotspot centroids into memory (avoids per-request PostGIS calls)
    try:
        await load_hotspot_cache()
    except Exception as exc:
        print(f"Hotspot cache load failed (non-fatal): {exc}")

    # Auto-seed if database is empty (first deploy on Render etc.)
    # Run in a background thread so uvicorn binds the port immediately
    # and Render's health check doesn't time out.
    try:
        from app.auto_seed import auto_seed_if_empty
        import threading
        seed_thread = threading.Thread(target=auto_seed_if_empty, daemon=True)
        seed_thread.start()
        print("[startup] Auto-seed check launched in background thread")
    except Exception as exc:
        print(f"Auto-seed check failed (non-fatal): {exc}")

    # Always seed elevation if the table is empty (idempotent upsert).
    try:
        import threading
        import sys
        scripts_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
        if os.path.abspath(scripts_dir) not in sys.path:
            sys.path.insert(0, os.path.abspath(scripts_dir))
        from seed_elevation_static import main as seed_elev
        elev_thread = threading.Thread(target=seed_elev, daemon=True)
        elev_thread.start()
        print("[startup] Elevation seed launched in background thread")
    except Exception as exc:
        print(f"Elevation seed failed (non-fatal): {exc}")

    # Start weather polling
    interval = int(os.getenv("RAIN_POLL_INTERVAL_SECONDS", "600"))
    scheduler.add_job(weather_poll_job, "interval", seconds=interval,
                      id="weather_poll", replace_existing=True)

    # Hathnikund discharge scraper — every hour
    scheduler.add_job(hathnikund_scrape_job, "interval", seconds=3600,
                      id="hathnikund_scrape", replace_existing=True)

    # Self-ping keep-alive — every 13 minutes during 6am–10pm IST
    scheduler.add_job(self_ping_job, "interval", minutes=13,
                      id="self_ping", replace_existing=True)

    scheduler.start()
    print(f"Weather polling started — every {interval}s")
    print("Hathnikund scraper started — every 3600s")
    print("[keep-alive] Self-ping scheduled every 13 minutes — Render instance will stay warm")

    # Trigger initial weather fetch so status isn't stuck at 'unknown'
    try:
        await weather_poll_job()
        print("Initial weather fetch completed")
    except Exception as exc:
        print(f"Initial weather fetch failed (non-fatal): {exc}")

    # Initial Hathnikund scrape
    try:
        await hathnikund_scrape_job()
        print("Initial Hathnikund scrape completed")
    except Exception as exc:
        print(f"Initial Hathnikund scrape failed (non-fatal): {exc}")


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


@app.get("/ping")
async def ping():
    """Lightweight keep-alive probe — no DB call."""
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/health")
async def health():
    """System status — DB connectivity, data counts, model metadata."""
    from app.services.yamuna import _cache as yamuna_cache
    base = {
        "model_version": "1.0.0-india-innovates-2026",
        "data_sources": ["SRTM v4.1", "OpenStreetMap", "Open-Meteo", "Datameet", "CWC"],
    }
    # Yamuna last-checked timestamp (from in-memory cache)
    last_yamuna = yamuna_cache.get("ts")
    base["last_yamuna_check"] = last_yamuna.isoformat() if last_yamuna else None

    try:
        async with async_session() as session:
            ward_row = await session.execute(text("SELECT COUNT(*) FROM wards"))
            hs_row   = await session.execute(text("SELECT COUNT(*) FROM hotspots"))
            wards    = ward_row.scalar() or 0
            hotspots = hs_row.scalar() or 0
        return {
            "status":   "ok",
            "db":       "connected",
            "wards":    wards,
            "hotspots": hotspots,
            **base,
        }
    except Exception:
        return {"status": "error", "db": "unreachable", "wards": None, "hotspots": None, **base}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """Real-time push: cycle events, weather updates."""
    await ws_manager.connect(ws)
    try:
        while True:
            # Keep connection alive; client can send pings
            await ws.receive_text()
    except (WebSocketDisconnect, Exception):
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

# ---------------------------------------------------------------------------
# Yamuna / Hathnikund advance warning endpoint (DB-backed)
# ---------------------------------------------------------------------------

@app.get("/yamuna/status")
async def yamuna_status():
    """Real-time Hathnikund discharge + Delhi flood advance warning."""
    from app.services.cwc_scraper import get_latest_reading
    from app.services.yamuna import fetch_yamuna_status
    from app.services.weather import get_weather_status
    try:
        # Try DB-backed reading first (most recent scraped value)
        async with engine.connect() as conn:
            db_reading = await get_latest_reading(conn)
        reading = db_reading or await fetch_yamuna_status()
        # Merge local Delhi rainfall from cached weather status
        wx = await get_weather_status()
        reading["local_rainfall_mm_hr"] = wx.get("local_rainfall_mm_hr")
        reading["rainfall_source"]      = wx.get("rainfall_source")
        return reading
    except Exception as exc:
        return {"error": str(exc), "alert_level": "UNKNOWN"}


async def self_ping_job():
    """
    Ping /ping to keep the Render free-tier instance warm 24/7.
    Fires every 13 minutes so the service never hits Render's 15-min
    inactivity sleep threshold.
    """
    ping_base = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:8000").rstrip("/")
    url = f"{ping_base}/ping"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
        print(f"[keep-alive] Ping {url} → {resp.status_code}")
    except Exception as exc:
        print(f"[keep-alive] Ping failed: {exc}")


async def hathnikund_scrape_job():
    """Hourly Hathnikund discharge scrape job."""
    try:
        from app.services.cwc_scraper import scrape_and_store
        async with engine.connect() as conn:
            await scrape_and_store(conn)
    except Exception as exc:
        print(f"[{datetime.now().isoformat()}] Hathnikund scrape error: {exc}")

# ---------------------------------------------------------------------------
# SSI endpoint + cache invalidation on cycle runs
# ---------------------------------------------------------------------------

@app.get("/ssi")
async def ssi_endpoint():
    """System Saturation Index — real-time composite stress score."""
    from app.services.ssi import compute_ssi
    try:
        async with engine.connect() as conn:
            result = await compute_ssi(conn)
        return result
    except Exception as exc:
        return {"error": str(exc), "ssi": 0, "level": "Unknown"}
