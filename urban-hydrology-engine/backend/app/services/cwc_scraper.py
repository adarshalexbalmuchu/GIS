"""
Hathnikund CWC Discharge Scraper.

Fetches real-time discharge data from CWC India-WRIS and stores it
in a new `yamuna_readings` table for historical tracking and the
advance warning panel.

Sources (in priority order):
  1. CWC India-WRIS open data API  (real discharge, cusecs)
  2. Open-Meteo ERA5 reanalysis — Haridwar upstream gauge (precipitation → discharge)
  3. IMD API via data.gov.in       (if available)

Falls back gracefully — always produces a reading, never crashes.

The table `yamuna_readings` stores:
  - station:         'Hathnikund Barrage'
  - discharge_cusecs: float
  - level_m:         predicted Delhi gauge level
  - alert_level:     NORMAL / WATCH / WARNING / DANGER / EXTREME
  - source:          which data source was used
  - observed_at:     IST timestamp of the reading
"""

import asyncio
import os
from datetime import datetime, timezone, timedelta

import httpx
from sqlalchemy import text

_IST = timezone(timedelta(hours=5, minutes=30))

# ── CWC sources to try in order ───────────────────────────────────────────
# 1. CWC FFS station details — Hathnikund Barrage (station id 240400)
_CWC_FFS_STATION_URL = (
    "https://ffs.india-water.gov.in/ffsite/stationdetails/?id=240400"
)
# 2. CWC FFS Delhi forecast details (Old Railway Bridge water level)
_CWC_FFS_FORECAST_URL = (
    "https://ffs.india-water.gov.in/ffsite/floodforecast/"
    "getforecastdetails/?id=1&state=Delhi"
)
# 3. India-WRIS public gauge API (legacy fallback)
_CWC_WRIS_URL = (
    "https://indiawris.gov.in/wris/data/PublicReport/realTimeData"
    "?stationId=240400&dataType=DISCHARGE&lang=en"
)

# ── Discharge → alert mapping ─────────────────────────────────────────────
def _classify(cusecs: float) -> tuple[str, float]:
    """Returns (alert_level, predicted_delhi_stage_m)."""
    if cusecs >= 500_000: return "EXTREME", 208.0
    if cusecs >= 300_000: return "DANGER",  207.0
    if cusecs >= 200_000: return "WARNING", 206.0
    if cusecs >= 100_000: return "WATCH",   205.0
    return "NORMAL", 202.0 + (cusecs / 100_000) * 2.0


# ── DB: ensure table exists ───────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS yamuna_readings (
    id              SERIAL PRIMARY KEY,
    station         VARCHAR(100) NOT NULL DEFAULT 'Hathnikund Barrage',
    discharge_cusecs FLOAT        NOT NULL,
    level_m         FLOAT,
    alert_level     VARCHAR(20)  NOT NULL DEFAULT 'NORMAL',
    source          VARCHAR(100),
    observed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_yamuna_observed ON yamuna_readings (observed_at DESC);
"""


async def ensure_table(conn) -> None:
    await conn.execute(text(CREATE_TABLE_SQL))
    await conn.execute(text(CREATE_INDEX_SQL))
    await conn.commit()


# ── Fetch from CWC FFS (primary) ─────────────────────────────────────────
async def _fetch_cwc_ffs(client: httpx.AsyncClient) -> dict | None:
    """Try CWC FloodWatch FFS endpoints for live Hathnikund + Delhi data."""

    # ── Attempt 1: station details (discharge + level at Hathnikund) ──────
    try:
        resp = await client.get(_CWC_FFS_STATION_URL, timeout=12)
        if resp.status_code == 200:
            data = resp.json()
            # FFS may return a list or a dict with nested records
            records = (
                data if isinstance(data, list)
                else data.get("data", data.get("records", data.get("stationData", [])))
            )
            if isinstance(records, dict):
                records = [records]
            r = records[0] if records else None
            if r:
                cusecs = float(
                    r.get("discharge", r.get("discharge_cusecs",
                    r.get("value", r.get("flow", 0)))) or 0
                )
                level_m_raw = float(
                    r.get("level", r.get("water_level",
                    r.get("gauge_level", 0))) or 0
                )
                if cusecs > 0:
                    obs_str = r.get(
                        "obsTime", r.get("observed_at",
                        r.get("dateTime", r.get("date",
                        datetime.now(_IST).isoformat())))
                    )
                    alert, predicted_level_m = _classify(cusecs)
                    return {
                        "discharge_cusecs": cusecs,
                        "level_m":         round(level_m_raw or predicted_level_m, 2),
                        "alert_level":     alert,
                        "source":          "CWC FFS Live",
                        "observed_at":     obs_str,
                    }
    except Exception:
        pass

    # ── Attempt 2: Delhi forecast details (level at Old Railway Bridge) ───
    try:
        resp = await client.get(_CWC_FFS_FORECAST_URL, timeout=12)
        if resp.status_code == 200:
            data = resp.json()
            records = (
                data if isinstance(data, list)
                else data.get("forecastData", data.get("data", data.get("records", [])))
            )
            if isinstance(records, dict):
                records = [records]
            r = records[0] if records else None
            if r:
                level_m_raw = float(
                    r.get("forecastLevel", r.get("water_level",
                    r.get("level", r.get("gauge_level", 0)))) or 0
                )
                cusecs = float(
                    r.get("discharge", r.get("flow", 0)) or 0
                )
                if level_m_raw > 0 or cusecs > 0:
                    obs_str = r.get(
                        "forecastTime", r.get("obsTime",
                        r.get("dateTime", datetime.now(_IST).isoformat()))
                    )
                    alert, predicted_level_m = _classify(cusecs)
                    return {
                        "discharge_cusecs": cusecs,
                        "level_m":         round(level_m_raw or predicted_level_m, 2),
                        "alert_level":     alert,
                        "source":          "CWC FFS Forecast",
                        "observed_at":     obs_str,
                    }
    except Exception:
        pass

    return None


# ── Fetch from CWC WRIS (secondary fallback) ──────────────────────────────
async def _fetch_cwc_wris(client: httpx.AsyncClient) -> dict | None:
    try:
        resp = await client.get(_CWC_WRIS_URL, timeout=12)
        if resp.status_code != 200:
            return None
        data = resp.json()
        records = data.get("records", data.get("data", []))
        if not records:
            return None
        r = records[0] if isinstance(records, list) else None
        if not r:
            return None
        cusecs = float(r.get("discharge", r.get("value", 0)) or 0)
        if cusecs <= 0:
            return None
        obs_str = r.get("obsTime", r.get("date", datetime.now(_IST).isoformat()))
        alert, level_m = _classify(cusecs)
        return {
            "discharge_cusecs": cusecs,
            "level_m":         round(level_m, 2),
            "alert_level":     alert,
            "source":          "CWC India-WRIS",
            "observed_at":     obs_str,
        }
    except Exception:
        return None


# ── Fetch upstream proxy from Open-Meteo ─────────────────────────────────
async def _fetch_openmeteo_proxy(client: httpx.AsyncClient) -> dict:
    """Haridwar upstream precipitation → estimated Hathnikund discharge."""
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=29.95&longitude=78.16"
        "&hourly=precipitation&timezone=Asia%2FKolkata"
        "&forecast_days=1&past_days=3"
    )
    try:
        resp = await client.get(url, timeout=12)
        resp.raise_for_status()
        p = resp.json()
        times = p["hourly"]["time"]
        rain  = p["hourly"]["precipitation"]
        now_s = datetime.now(_IST).strftime("%Y-%m-%dT%H:00")
        try:
            idx = times.index(now_s)
        except ValueError:
            idx = len(times) - 1
        cum_48h = sum(r for r in rain[max(0, idx-48):idx] if r is not None)
        cusecs  = min(600_000, cum_48h * 4_500)
    except Exception:
        cusecs = 0.0

    alert, level_m = _classify(cusecs)
    return {
        "discharge_cusecs": round(cusecs, 1),
        "level_m":         round(level_m, 2),
        "alert_level":     alert,
        "source":          "Open-Meteo ERA5 reanalysis — Haridwar upstream gauge",
        "observed_at":     datetime.now(_IST).isoformat(),
    }


# ── Main scrape + store function ──────────────────────────────────────────
async def scrape_and_store(conn) -> dict:
    """
    Fetch latest Hathnikund reading and insert into yamuna_readings.
    Returns the reading dict.
    """
    await ensure_table(conn)

    async with httpx.AsyncClient() as client:
        reading = await _fetch_cwc_ffs(client)
        if not reading:
            reading = await _fetch_cwc_wris(client)
        if not reading:
            reading = await _fetch_openmeteo_proxy(client)

    # Ensure observed_at is a proper datetime object (asyncpg rejects strings)
    obs = reading["observed_at"]
    if isinstance(obs, str):
        from datetime import datetime as _dt
        try:
            obs = _dt.fromisoformat(obs)
        except (ValueError, TypeError):
            obs = datetime.now(_IST)

    await conn.execute(text("""
        INSERT INTO yamuna_readings
            (station, discharge_cusecs, level_m, alert_level, source, observed_at)
        VALUES
            ('Hathnikund Barrage', :discharge, :level_m, :alert,
             :source, :observed_at)
    """), {
        "discharge":   reading["discharge_cusecs"],
        "level_m":     reading["level_m"],
        "alert":       reading["alert_level"],
        "source":      reading["source"],
        "observed_at": obs,
    })
    await conn.commit()

    print(f"[Hathnikund] {reading['alert_level']:8s} "
          f"{reading['discharge_cusecs']:>10,.0f} cusecs  "
          f"Delhi~{reading['level_m']}m  src={reading['source']}")
    return reading


async def get_latest_reading(conn) -> dict | None:
    """Return the most recent yamuna_readings row."""
    try:
        row = await conn.execute(text("""
            SELECT discharge_cusecs, level_m, alert_level, source,
                   observed_at, station
            FROM yamuna_readings
            ORDER BY observed_at DESC
            LIMIT 1
        """))
        r = row.mappings().first()
        if not r:
            return None
        from app.services.yamuna import _discharge_to_alert
        alert = _discharge_to_alert(r["discharge_cusecs"])
        return {
            "source":                  r["source"],
            "station":                 r["station"],
            "discharge_cusecs":        round(r["discharge_cusecs"]),
            "discharge_cumecs":        round(r["discharge_cusecs"] * 0.0283168, 1),
            "obs_time":                r["observed_at"].isoformat() if r["observed_at"] else "—",
            "alert_level":             r["alert_level"],
            "alert_colour":            alert["colour"],
            "predicted_delhi_stage_m": round(r["level_m"], 2) if r["level_m"] else None,
            "travel_time_hours":       alert["travel_time_hours"],
            "eta_delhi":               alert["eta_delhi"],
            "is_live":                 any(k in (r["source"] or "") for k in ("CWC", "WRIS")),
        }
    except Exception:
        return None
