"""
Map-state, run/cycle, weather status, history API routes.
"""

import json
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
from typing import Optional

from app.auth import verify_api_key
from app.db import async_session, engine
from app.services.scoring import compute_ward_score
from app.services.dispatch_lp import run_dispatch
from app.services.weather import get_weather_status
from app.ws import manager as ws_manager

router = APIRouter()


# ---------------------------------------------------------------------------
# Internal cycle logic (callable by HTTP handler + scheduler)
# ---------------------------------------------------------------------------

async def run_cycle_internal(conn: AsyncConnection) -> dict:
    """Score all wards and dispatch pumps. Returns summary dict."""
    dispatches: list[dict] = []
    safe_count = 0

    wards = (await conn.execute(
        text("SELECT id, name FROM wards ORDER BY id")
    )).mappings().fetchall()

    for w in wards:
        score = await compute_ward_score(w["id"], conn)

        if score["triggered"]:
            hs_rows = await conn.execute(
                text("""
                    SELECT id, capacity_c, priority_weight, critical_penalty_pc
                    FROM hotspots
                    WHERE ward_id = :wid
                    ORDER BY priority_weight DESC
                    LIMIT 30
                """),
                {"wid": w["id"]},
            )
            hotspots = [dict(r) for r in hs_rows.mappings().fetchall()]

            result = run_dispatch(
                ward_id=w["id"],
                ward_name=w["name"],
                ws_score=score["ws_score"],
                hotspots=hotspots,
            )

            dispatches.append({
                "ward_name": w["name"],
                "ws_score": score["ws_score"],
                "dispatch_message": result["dispatch_message"],
            })
            result_json = result
            status = "critical" if score["ws_score"] < 0 else "dispatched"
        else:
            result_json = {"message": "Ward score nominal"}
            status = "safe"
            safe_count += 1

        await conn.execute(
            text("""
                INSERT INTO dispatch_runs
                    (ward_id, ws_score, status, result_json, created_at)
                VALUES (:wid, :ws, :st, :rj, now())
            """),
            {
                "wid": w["id"],
                "ws": score["ws_score"],
                "st": status,
                "rj": json.dumps(result_json),
            },
        )

    await conn.commit()

    return {
        "cycle_completed_at": datetime.utcnow().isoformat(),
        "wards_scored": len(wards),
        "wards_triggered": len(dispatches),
        "dispatches": dispatches,
        "safe_wards": safe_count,
    }


# ---------------------------------------------------------------------------
# POST /run/cycle
# ---------------------------------------------------------------------------

@router.post("/run/cycle")
async def run_cycle(_auth=Depends(verify_api_key)):
    """Score all wards; dispatch pumps where triggered."""
    async with engine.connect() as conn:
        result = await run_cycle_internal(conn)
    # Broadcast to WebSocket clients
    await ws_manager.broadcast({
        "type": "cycle",
        "wards_triggered": result["wards_triggered"],
        "safe_wards": result["safe_wards"],
    })
    return result


# ---------------------------------------------------------------------------
# GET /map/state
# ---------------------------------------------------------------------------

def _ward_color(ws_score: float | None) -> str:
    if ws_score is None:
        return "grey"
    if ws_score >= 80:
        return "green"
    if ws_score >= 70:
        return "yellow"
    return "red"


@router.get("/city/bounds")
async def city_bounds():
    """Return bounding box of all wards + a central rain polygon (~40% area)."""
    async with engine.connect() as conn:
        row = (await conn.execute(text("""
            SELECT ST_XMin(ext) AS xmin, ST_YMin(ext) AS ymin,
                   ST_XMax(ext) AS xmax, ST_YMax(ext) AS ymax,
                   (SELECT count(*) FROM hotspots) AS hotspot_count
            FROM (SELECT ST_Extent(geom) AS ext FROM wards) sub
        """))).mappings().first()

        xmin, ymin = row["xmin"], row["ymin"]
        xmax, ymax = row["xmax"], row["ymax"]

        # Center lat/lng and zoom-friendly bbox
        center_lat = (ymin + ymax) / 2
        center_lng = (xmin + xmax) / 2

        # Shrink 30% on each side for rain polygon
        dx = (xmax - xmin) * 0.30
        dy = (ymax - ymin) * 0.30
        rain_polygon = {
            "type": "Polygon",
            "coordinates": [[
                [xmin + dx, ymin + dy],
                [xmax - dx, ymin + dy],
                [xmax - dx, ymax - dy],
                [xmin + dx, ymax - dy],
                [xmin + dx, ymin + dy],
            ]],
        }

    return {
        "center": [center_lat, center_lng],
        "bbox": [xmin, ymin, xmax, ymax],
        "rain_polygon": rain_polygon,
        "hotspot_count": row["hotspot_count"],
    }


@router.post("/reset")
async def reset_city(_auth=Depends(verify_api_key)):
    """Reset all event data and restore hotspot capacity for live demos."""
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM dispatch_runs"))
        await conn.execute(text("DELETE FROM sensor_events"))
        await conn.execute(text("DELETE FROM rain_events"))
        await conn.execute(text("UPDATE hotspots SET capacity_c = 80.0"))
        ward_count = (await conn.execute(text("SELECT count(*) FROM wards"))).scalar()
    return {"message": "City reset to baseline", "wards_reset": ward_count}


@router.get("/map/state")
async def map_state():
    """Return GeoJSON FeatureCollection of wards + hotspot list."""
    async with engine.connect() as conn:
        # ── Ward features with latest dispatch_run ───────────────────
        ward_rows = await conn.execute(text("""
            SELECT
                w.id            AS ward_id,
                w.name          AS ward_name,
                ST_AsGeoJSON(w.geom)::json AS geometry,
                dr.ws_score,
                dr.status,
                dr.result_json->>'dispatch_message' AS dispatch_message,
                we.terrain_class,
                we.mean_elevation,
                we.runoff_t AS terrain_runoff_t,
                we.mean_slope
            FROM wards w
            LEFT JOIN LATERAL (
                SELECT ws_score, status, result_json
                FROM dispatch_runs
                WHERE ward_id = w.id
                ORDER BY created_at DESC
                LIMIT 1
            ) dr ON true
            LEFT JOIN ward_elevation we ON we.ward_id = w.id
            ORDER BY w.id
        """))
        ward_features = []
        for r in ward_rows.mappings().fetchall():
            ws = r["ws_score"]
            ward_features.append({
                "type": "Feature",
                "geometry": r["geometry"],
                "properties": {
                    "ward_id": r["ward_id"],
                    "ward_name": r["ward_name"],
                    "ws_score": ws,
                    "status": r["status"] or "no_data",
                    "dispatch_message": r["dispatch_message"] or "",
                    "color": _ward_color(ws),
                    "terrain_class": r["terrain_class"],
                    "mean_elevation": round(r["mean_elevation"], 1) if r["mean_elevation"] is not None else None,
                    "terrain_runoff_t": r["terrain_runoff_t"],
                    "mean_slope": round(r["mean_slope"], 2) if r["mean_slope"] is not None else None,
                },
            })

    return {
        "type": "FeatureCollection",
        "features": ward_features,
    }


# ---------------------------------------------------------------------------
# GET /weather/status — public, no auth
# ---------------------------------------------------------------------------

@router.get("/weather/status")
async def weather_status():
    """Return current weather conditions from last OWM poll."""
    return await get_weather_status()


# ---------------------------------------------------------------------------
# GET /map/infrastructure — critical infrastructure GeoJSON
# ---------------------------------------------------------------------------

ICON_MAP = {"hospital": "\U0001f3e5", "substation": "\u26a1", "fire_station": "\U0001f692"}

@router.get("/map/infrastructure")
async def map_infrastructure():
    """Return GeoJSON FeatureCollection of all critical infrastructure."""
    async with engine.connect() as conn:
        rows = await conn.execute(text("""
            SELECT facility_type, name, lon, lat
            FROM critical_infrastructure
            ORDER BY facility_type, name
        """))
        features = []
        counts = {}
        for r in rows.mappings().fetchall():
            ft = r["facility_type"]
            counts[ft] = counts.get(ft, 0) + 1
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                "properties": {
                    "facility_type": ft,
                    "name": r["name"] or "unnamed",
                    "icon": ICON_MAP.get(ft, ""),
                },
            })
    return {
        "type": "FeatureCollection",
        "features": features,
        "counts": counts,
    }


# ---------------------------------------------------------------------------
# GET /map/elevation — terrain choropleth
# ---------------------------------------------------------------------------

TERRAIN_COLORS = {
    "floodplain": "#2166ac",
    "flat":       "#74add1",
    "moderate":   "#fee090",
    "steep":      "#f46d43",
    "ridge":      "#a50026",
}

@router.get("/map/elevation")
async def map_elevation():
    """Return ward GeoJSON coloured by terrain class."""
    async with engine.connect() as conn:
        rows = await conn.execute(text("""
            SELECT
                w.id            AS ward_id,
                w.name          AS ward_name,
                ST_AsGeoJSON(w.geom)::json AS geometry,
                we.terrain_class,
                we.mean_elevation,
                we.mean_slope,
                we.runoff_t,
                we.elevation_range
            FROM wards w
            LEFT JOIN ward_elevation we ON we.ward_id = w.id
            ORDER BY w.id
        """))

        features = []
        terrain_counts = {}
        for r in rows.mappings().fetchall():
            tc = r["terrain_class"] or "unknown"
            terrain_counts[tc] = terrain_counts.get(tc, 0) + 1
            features.append({
                "type": "Feature",
                "geometry": r["geometry"],
                "properties": {
                    "ward_id": r["ward_id"],
                    "ward_name": r["ward_name"],
                    "terrain_class": tc,
                    "mean_elevation": round(r["mean_elevation"], 1) if r["mean_elevation"] is not None else None,
                    "mean_slope": round(r["mean_slope"], 2) if r["mean_slope"] is not None else None,
                    "runoff_t": r["runoff_t"],
                    "elevation_range": round(r["elevation_range"], 1) if r["elevation_range"] is not None else None,
                    "color": TERRAIN_COLORS.get(tc, "#555555"),
                },
            })

    return {
        "type": "FeatureCollection",
        "features": features,
        "terrain_counts": terrain_counts,
    }


# ---------------------------------------------------------------------------
# GET /cycle/latest — public, no auth
# ---------------------------------------------------------------------------

@router.get("/cycle/latest")
async def cycle_latest():
    """Return the timestamp of the most recent dispatch_run."""
    async with engine.connect() as conn:
        row = await conn.execute(text(
            "SELECT created_at FROM dispatch_runs ORDER BY created_at DESC LIMIT 1"
        ))
        result = row.scalar_one_or_none()
    return {
        "last_cycle_at": result.isoformat() if result else None,
    }


# ---------------------------------------------------------------------------
# GET /history/timeline — daily aggregated trigger counts
# ---------------------------------------------------------------------------

def _intensity_label(trigger_pct: float) -> str:
    if trigger_pct >= 0.60:
        return "extreme"
    if trigger_pct >= 0.45:
        return "severe"
    if trigger_pct >= 0.30:
        return "heavy"
    if trigger_pct >= 0.15:
        return "moderate"
    return "light"


@router.get("/history/timeline")
async def history_timeline(
    days: int = Query(30, ge=1, le=365),
    year: Optional[int] = Query(None, ge=2020, le=2030),
):
    """
    Return daily aggregated dispatch counts.
    When year is provided, uses that year's monsoon date range (Jun 1 – Sep 30).
    Otherwise uses rolling window from now.
    """
    async with engine.connect() as conn:
        if year:
            params = {"start": date(year, 6, 1), "end": date(year, 10, 1)}
            where = "WHERE created_at >= :start AND created_at < :end"
        else:
            cutoff = datetime.utcnow() - timedelta(days=days)
            params = {"cutoff": cutoff}
            where = "WHERE created_at >= :cutoff"

        rows = await conn.execute(text(f"""
            SELECT
                date_trunc('day', created_at)::date AS day,
                count(*) FILTER (WHERE status = 'critical')   AS critical_count,
                count(*) FILTER (WHERE status = 'dispatched') AS dispatched_count,
                count(*) FILTER (WHERE status = 'safe')       AS safe_count,
                count(*)                                       AS total
            FROM dispatch_runs
            {where}
            GROUP BY 1
            ORDER BY 1
        """), params)

        timeline = []
        for r in rows.mappings().fetchall():
            total = r["total"]
            triggered = r["critical_count"] + r["dispatched_count"]
            trigger_pct = triggered / total if total else 0
            timeline.append({
                "date": r["day"].isoformat(),
                "critical": r["critical_count"],
                "dispatched": r["dispatched_count"],
                "safe": r["safe_count"],
                "total": total,
                "triggered": triggered,
                "intensity_label": _intensity_label(trigger_pct),
            })

    return {"days": days, "year": year, "timeline": timeline}


# ---------------------------------------------------------------------------
# GET /history/worst-wards — most frequently triggered wards
# ---------------------------------------------------------------------------

@router.get("/history/worst-wards")
async def history_worst_wards(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(10, ge=1, le=50),
    year: Optional[int] = Query(None, ge=2020, le=2030),
):
    """
    Return the top N wards with the most triggered dispatch_runs.
    """
    async with engine.connect() as conn:
        if year:
            params = {"start": date(year, 6, 1), "end": date(year, 10, 1), "lim": limit}
            where = "WHERE dr.created_at >= :start AND dr.created_at < :end"
        else:
            cutoff = datetime.utcnow() - timedelta(days=days)
            params = {"cutoff": cutoff, "lim": limit}
            where = "WHERE dr.created_at >= :cutoff"

        rows = await conn.execute(text(f"""
            SELECT
                w.id                                            AS ward_id,
                w.name                                          AS ward_name,
                w.zone_name,
                count(*) FILTER (WHERE dr.status IN ('critical','dispatched')) AS trigger_count,
                count(*) FILTER (WHERE dr.status = 'critical')                AS critical_count,
                round(avg(dr.ws_score)::numeric, 1)                           AS avg_score,
                ST_Y(ST_Centroid(w.geom))                                     AS lat,
                ST_X(ST_Centroid(w.geom))                                     AS lng
            FROM wards w
            JOIN dispatch_runs dr ON dr.ward_id = w.id
            {where}
            GROUP BY w.id, w.name, w.zone_name, w.geom
            ORDER BY trigger_count DESC, critical_count DESC
            LIMIT :lim
        """), params)

        wards = []
        for r in rows.mappings().fetchall():
            wards.append({
                "ward_id": r["ward_id"],
                "ward_name": r["ward_name"],
                "zone_name": r["zone_name"],
                "trigger_count": r["trigger_count"],
                "critical_count": r["critical_count"],
                "avg_score": float(r["avg_score"]) if r["avg_score"] is not None else None,
                "lat": r["lat"],
                "lng": r["lng"],
            })

    return {"days": days, "year": year, "limit": limit, "wards": wards}


# ---------------------------------------------------------------------------
# GET /history/summary — high-level stats for the time window
# ---------------------------------------------------------------------------

@router.get("/history/summary")
async def history_summary(
    days: int = Query(30, ge=1, le=365),
    year: Optional[int] = Query(None, ge=2020, le=2030),
):
    """
    Summary stats: peak day, avg daily triggered, total dispatches, most affected zone.
    """
    async with engine.connect() as conn:
        if year:
            params = {"start": date(year, 6, 1), "end": date(year, 10, 1)}
            where = "WHERE created_at >= :start AND created_at < :end"
            where_dr = "WHERE dr.created_at >= :start AND dr.created_at < :end"
        else:
            cutoff = datetime.utcnow() - timedelta(days=days)
            params = {"cutoff": cutoff}
            where = "WHERE created_at >= :cutoff"
            where_dr = "WHERE dr.created_at >= :cutoff"

        # Peak day
        peak_row = (await conn.execute(text(f"""
            SELECT
                date_trunc('day', created_at)::date AS day,
                count(*) FILTER (WHERE status IN ('critical','dispatched')) AS triggered
            FROM dispatch_runs
            {where}
            GROUP BY 1
            ORDER BY triggered DESC
            LIMIT 1
        """), params)).mappings().first()

        # Averages & totals
        agg_row = (await conn.execute(text(f"""
            SELECT
                count(DISTINCT date_trunc('day', created_at)) AS active_days,
                count(*) FILTER (WHERE status IN ('critical','dispatched')) AS total_triggered,
                count(*) FILTER (WHERE status = 'critical') AS total_critical,
                count(*) AS total_runs
            FROM dispatch_runs
            {where}
        """), params)).mappings().first()

        # Most affected zone
        zone_row = (await conn.execute(text(f"""
            SELECT w.zone_name, count(*) AS cnt
            FROM dispatch_runs dr
            JOIN wards w ON w.id = dr.ward_id
            {where_dr}
              AND dr.status IN ('critical','dispatched')
              AND w.zone_name IS NOT NULL
            GROUP BY w.zone_name
            ORDER BY cnt DESC
            LIMIT 1
        """), params)).mappings().first()

    active_days = agg_row["active_days"] if agg_row else 0
    total_triggered = agg_row["total_triggered"] if agg_row else 0

    return {
        "days": days,
        "year": year,
        "peak_day": peak_row["day"].isoformat() if peak_row else None,
        "peak_day_triggered": peak_row["triggered"] if peak_row else 0,
        "avg_daily_triggered": round(total_triggered / active_days, 1) if active_days else 0,
        "total_dispatches": total_triggered,
        "total_critical": agg_row["total_critical"] if agg_row else 0,
        "total_runs": agg_row["total_runs"] if agg_row else 0,
        "most_affected_zone": zone_row["zone_name"] if zone_row else None,
        "most_affected_zone_count": zone_row["cnt"] if zone_row else 0,
    }


# ---------------------------------------------------------------------------
# GET /history/seasons — available data seasons
# ---------------------------------------------------------------------------

@router.get("/history/seasons")
async def history_seasons():
    """Return available monsoon data seasons with metadata."""
    async with engine.connect() as conn:
        # Detect 2024 monsoon data
        row_2024 = (await conn.execute(text("""
            SELECT
                count(*)                                       AS total_runs,
                count(DISTINCT date_trunc('day', created_at))  AS total_days,
                count(*) FILTER (WHERE status IN ('critical','dispatched')) AS total_triggered
            FROM dispatch_runs
            WHERE created_at BETWEEN '2024-06-01' AND '2024-10-01'
        """))).mappings().first()

        seasons = []
        if row_2024 and row_2024["total_runs"] > 0:
            # Get peak day for 2024
            peak = (await conn.execute(text("""
                SELECT
                    date_trunc('day', created_at)::date AS day,
                    count(*) FILTER (WHERE status IN ('critical','dispatched')) AS triggered
                FROM dispatch_runs
                WHERE created_at BETWEEN '2024-06-01' AND '2024-10-01'
                GROUP BY 1
                ORDER BY triggered DESC
                LIMIT 1
            """))).mappings().first()

            seasons.append({
                "year": 2024,
                "label": "Monsoon 2024",
                "start": "2024-06-01",
                "end": "2024-09-30",
                "total_days": row_2024["total_days"],
                "total_dispatches": row_2024["total_runs"],
                "total_triggered": row_2024["total_triggered"],
                "peak_day": peak["day"].isoformat() if peak else None,
                "peak_triggered": peak["triggered"] if peak else 0,
                "source": "Open-Meteo + real rainfall data",
            })

        # Detect recent data (last 30 days)
        row_recent = (await conn.execute(text("""
            SELECT count(*) AS cnt
            FROM dispatch_runs
            WHERE created_at >= NOW() - INTERVAL '30 days'
        """))).mappings().first()

        if row_recent and row_recent["cnt"] > 0:
            seasons.append({
                "year": None,
                "label": "Recent (last 30 days)",
                "total_dispatches": row_recent["cnt"],
                "source": "Live test cycles",
            })

    return {"seasons": seasons}


# ---------------------------------------------------------------------------
# GET /report/snapshot — printable HTML status report
# ---------------------------------------------------------------------------

@router.get("/report/snapshot", response_class=HTMLResponse)
async def report_snapshot():
    """Generate a printable HTML snapshot report of current city status."""
    now = datetime.utcnow() + timedelta(hours=5, minutes=30)  # IST
    ts = now.strftime("%d %b %Y, %H:%M IST")

    async with engine.connect() as conn:
        # Ward summary
        ward_count_row = (await conn.execute(text("SELECT count(*) AS c FROM wards"))).mappings().first()
        ward_count = ward_count_row["c"] if ward_count_row else 0

        # Latest cycle dispatches
        latest_cycle = (await conn.execute(text("""
            SELECT created_at FROM dispatch_runs ORDER BY created_at DESC LIMIT 1
        """))).mappings().first()

        cycle_ts = "No cycles run"
        triggered_wards = []
        safe_count = 0
        total_pumps = 0

        if latest_cycle:
            ct = latest_cycle["created_at"]
            cycle_ts = ct.strftime("%d %b %Y, %H:%M IST") if ct else "N/A"

            rows = (await conn.execute(text("""
                SELECT w.name AS ward_name, w.zone_name,
                       d.ws_score, d.status,
                       d.result_json->>'dispatch_message' AS dispatch_message
                FROM dispatch_runs d
                JOIN wards w ON w.id = d.ward_id
                WHERE d.created_at = (SELECT max(created_at) FROM dispatch_runs)
                ORDER BY d.ws_score ASC
            """))).mappings().all()

            for r in rows:
                if r["status"] in ("critical", "dispatched"):
                    import re
                    pmatch = re.search(r"(\d+) pumps", r["dispatch_message"] or "")
                    pumps = int(pmatch.group(1)) if pmatch else 0
                    total_pumps += pumps
                    triggered_wards.append({
                        "ward": r["ward_name"],
                        "zone": r["zone_name"] or "",
                        "score": f"{r['ws_score']:.1f}" if r["ws_score"] is not None else "N/A",
                        "status": r["status"].upper(),
                        "pumps": pumps,
                    })
                else:
                    safe_count += 1

        # Infrastructure counts
        infra = (await conn.execute(text("""
            SELECT facility_type, count(*) AS c
            FROM critical_infrastructure
            GROUP BY facility_type
        """))).mappings().all()
        infra_counts = {r["facility_type"]: r["c"] for r in infra}

        # Build HTML
        triggered_rows = ""
        for i, w in enumerate(triggered_wards, 1):
            status_color = "#991B1B" if w["status"] == "CRITICAL" else "#D97706"
            triggered_rows += f"""
            <tr>
                <td style="padding:6px 10px;border-bottom:1px solid #E2E8F0;font-size:13px">{i}</td>
                <td style="padding:6px 10px;border-bottom:1px solid #E2E8F0;font-size:13px">{w['ward']}<br><span style="color:#94A3B8;font-size:11px">{w['zone']}</span></td>
                <td style="padding:6px 10px;border-bottom:1px solid #E2E8F0;font-family:monospace;font-weight:700;font-size:13px">{w['score']}</td>
                <td style="padding:6px 10px;border-bottom:1px solid #E2E8F0;color:{status_color};font-weight:600;font-size:12px">{w['status']}</td>
                <td style="padding:6px 10px;border-bottom:1px solid #E2E8F0;font-family:monospace;font-size:13px">{w['pumps']}</td>
            </tr>"""

        if not triggered_rows:
            triggered_rows = '<tr><td colspan="5" style="padding:20px;text-align:center;color:#94A3B8;font-size:13px">No wards currently triggered — all nominal</td></tr>'

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Flood Intelligence Report — {ts}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:'DM Sans',system-ui,sans-serif; color:#1B2A4A; background:#fff; padding:40px; max-width:800px; margin:0 auto; }}
  @media print {{
    body {{ padding:20px; }}
    .no-print {{ display:none !important; }}
  }}
  .report-header {{ border-bottom:3px solid #1B2A4A; padding-bottom:16px; margin-bottom:24px; }}
  .report-header h1 {{ font-size:22px; font-weight:700; color:#1B2A4A; }}
  .report-header p {{ font-size:12px; color:#6B7B8D; margin-top:4px; }}
  .report-header .ts {{ font-family:'DM Mono',monospace; font-size:13px; color:#00BCD4; margin-top:4px; }}
  .stat-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:28px; }}
  .stat-box {{ background:#F8F9FB; border:1px solid #E2E8F0; border-radius:8px; padding:14px; }}
  .stat-box .label {{ font-size:10px; font-weight:600; color:#94A3B8; text-transform:uppercase; letter-spacing:.6px; }}
  .stat-box .val {{ font-size:24px; font-weight:700; font-family:'DM Mono',monospace; margin-top:4px; }}
  .stat-box .val.red {{ color:#DC2626; }}
  .stat-box .val.green {{ color:#16A34A; }}
  .stat-box .val.cyan {{ color:#00BCD4; }}
  h2 {{ font-size:15px; font-weight:700; color:#1B2A4A; margin:24px 0 12px; padding-bottom:6px; border-bottom:1px solid #E2E8F0; }}
  table {{ width:100%; border-collapse:collapse; }}
  th {{ text-align:left; padding:8px 10px; background:#F8F9FB; border-bottom:2px solid #E2E8F0; font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:#6B7B8D; }}
  .footer {{ margin-top:32px; padding-top:12px; border-top:1px solid #E2E8F0; font-size:11px; color:#94A3B8; display:flex; justify-content:space-between; }}
  .btn-print {{ background:#1B2A4A; color:#fff; border:none; padding:10px 24px; border-radius:6px; font-size:13px; font-weight:600; cursor:pointer; font-family:'DM Sans'; }}
  .btn-print:hover {{ background:#2D3E5F; }}
</style>
</head>
<body>
<div class="report-header">
  <h1>Delhi Flood Intelligence — Status Report</h1>
  <p>Municipal Hydrology Command &middot; Urban Hydrology Engine</p>
  <div class="ts">Generated: {ts} &nbsp;|&nbsp; Last Cycle: {cycle_ts}</div>
</div>

<div class="stat-grid">
  <div class="stat-box">
    <div class="label">Wards Monitored</div>
    <div class="val cyan">{ward_count}</div>
  </div>
  <div class="stat-box">
    <div class="label">Triggered</div>
    <div class="val red">{len(triggered_wards)}</div>
  </div>
  <div class="stat-box">
    <div class="label">Safe</div>
    <div class="val green">{safe_count}</div>
  </div>
  <div class="stat-box">
    <div class="label">Pumps Dispatched</div>
    <div class="val">{total_pumps}</div>
  </div>
</div>

<h2>Triggered Wards — Dispatch Summary</h2>
<table>
  <thead><tr><th>#</th><th>Ward</th><th>PMRS Score</th><th>Status</th><th>Pumps</th></tr></thead>
  <tbody>{triggered_rows}</tbody>
</table>

<h2>Infrastructure Coverage</h2>
<div class="stat-grid" style="grid-template-columns:repeat(3,1fr)">
  <div class="stat-box"><div class="label">Hospitals</div><div class="val">{infra_counts.get('hospital', 0)}</div></div>
  <div class="stat-box"><div class="label">Substations</div><div class="val">{infra_counts.get('substation', 0)}</div></div>
  <div class="stat-box"><div class="label">Fire Stations</div><div class="val">{infra_counts.get('fire_station', 0)}</div></div>
</div>

<div class="footer">
  <span>Urban Hydrology Engine v1.0 &middot; PostGIS + FastAPI + PuLP</span>
  <button class="btn-print no-print" onclick="window.print()">Print / Save PDF</button>
</div>
</body>
</html>"""

    return HTMLResponse(content=html)
