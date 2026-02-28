"""
Ward drill-down detail endpoint.
GET /ward/{ward_id}/detail — returns per-ward timeline, infra, terrain.
"""

from fastapi import APIRouter, Query
from sqlalchemy import text

from app.db import engine

router = APIRouter()


@router.get("/ward/{ward_id}/detail")
async def ward_detail(ward_id: int, days: int = Query(30, ge=1, le=365)):
    """Return comprehensive detail for a single ward."""
    async with engine.connect() as conn:
        # ── Ward basic info + elevation ──────────────────────────
        ward = (await conn.execute(text("""
            SELECT w.id, w.name, w.zone_name, w.ward_no,
                   we.terrain_class, we.mean_elevation, we.mean_slope,
                   we.runoff_t
            FROM wards w
            LEFT JOIN ward_elevation we ON we.ward_id = w.id
            WHERE w.id = :wid
        """), {"wid": ward_id})).mappings().first()

        if not ward:
            return {"error": "Ward not found"}

        # ── Hotspot counts ───────────────────────────────────────
        hs = (await conn.execute(text("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE critical_penalty_pc > 0) AS critical
            FROM hotspots WHERE ward_id = :wid
        """), {"wid": ward_id})).mappings().first()

        hotspot_count = hs["total"] if hs else 0
        critical_hotspots = hs["critical"] if hs else 0

        # ── Nearby infrastructure (within 500m of any hotspot) ───
        infra = (await conn.execute(text("""
            SELECT ci.facility_type, COUNT(DISTINCT ci.id) AS cnt
            FROM critical_infrastructure ci
            JOIN hotspots h ON h.ward_id = :wid
            WHERE ST_DWithin(ci.geom::geography, h.geom::geography, 500)
            GROUP BY ci.facility_type
        """), {"wid": ward_id})).mappings().all()

        infra_map = {r["facility_type"]: r["cnt"] for r in infra}

        # ── Score history (most recent 30 days that have data) ────
        history = (await conn.execute(text("""
            SELECT day, ws_score, status, pumps_dispatched
            FROM (
                SELECT created_at::date AS day,
                       AVG(ws_score) AS ws_score,
                       MAX(status) AS status,
                       SUM(CASE WHEN result_json->>'pumps_total' IS NOT NULL
                           THEN (result_json->>'pumps_total')::int ELSE 0 END) AS pumps_dispatched
                FROM dispatch_runs
                WHERE ward_id = :wid
                GROUP BY created_at::date
                ORDER BY day DESC
                LIMIT :d
            ) sub
            ORDER BY day ASC
        """), {"wid": ward_id, "d": days})).mappings().all()

        score_history = []
        for h in history:
            sc = float(h["ws_score"]) if h["ws_score"] is not None else None
            st = h["status"] or "safe"
            pd = h["pumps_dispatched"] or 0
            score_history.append({
                "date": h["day"].isoformat(),
                "ws_score": round(sc, 1) if sc is not None else None,
                "status": st,
                "pumps_dispatched": pd,
            })

        # ── Aggregated stats from ALL history ────────────────────
        stats = (await conn.execute(text("""
            SELECT MIN(ws_score) AS worst_score,
                   (array_agg(created_at::date ORDER BY ws_score ASC))[1] AS worst_date,
                   COUNT(*) FILTER (WHERE status IN ('critical','dispatched')) AS times_triggered,
                   COUNT(*) FILTER (WHERE status = 'critical') AS times_critical
            FROM (
                SELECT created_at::date,
                       AVG(ws_score) AS ws_score,
                       MAX(status) AS status
                FROM dispatch_runs
                WHERE ward_id = :wid
                GROUP BY created_at::date
            ) daily
        """), {"wid": ward_id})).mappings().first()

        worst_score = round(float(stats["worst_score"]), 1) if stats and stats["worst_score"] is not None else None
        worst_date = stats["worst_date"].isoformat() if stats and stats["worst_date"] else None
        times_triggered = stats["times_triggered"] if stats else 0
        times_critical = stats["times_critical"] if stats else 0

        return {
            "ward_id": ward["id"],
            "ward_name": ward["name"],
            "zone_name": ward["zone_name"],
            "terrain_class": ward["terrain_class"],
            "mean_elevation": round(float(ward["mean_elevation"]), 1) if ward["mean_elevation"] else None,
            "runoff_t": round(float(ward["runoff_t"]), 1) if ward["runoff_t"] else None,
            "hotspot_count": hotspot_count,
            "critical_hotspots": critical_hotspots,
            "nearby_infrastructure": {
                "hospitals": infra_map.get("hospital", 0),
                "substations": infra_map.get("substation", 0),
                "fire_stations": infra_map.get("fire_station", 0),
            },
            "score_history": score_history,
            "worst_score": round(worst_score, 1) if worst_score is not None else None,
            "worst_score_display": "CRITICAL" if (worst_score is not None and worst_score <= -999) else (
                str(round(worst_score, 1)) if worst_score is not None else None
            ),
            "worst_date": worst_date,
            "times_triggered_30d": times_triggered,
            "times_critical_30d": times_critical,
        }
