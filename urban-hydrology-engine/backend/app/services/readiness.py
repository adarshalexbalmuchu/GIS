"""
Pre-Monsoon Readiness Score (PMRS-Static) engine.

Unlike the real-time PMRS (which scores wards *during* rainfall),
the Readiness Score is a *pre-event* static index computed from
historical patterns and infrastructure state — not live rain events.

Formula (0–100, higher = more ready):
  PMRS_static = 100
              - 40 * flood_risk_norm        # historical flood frequency
              - 30 * (1 - capacity_norm)    # drain capacity deficit
              - 20 * vulnerability_norm     # terrain + elevation risk
              - 10 * infra_exposure_norm    # critical infra at risk

Returns a per-ward score, band (green/amber/red), and contributing factors.
"""

from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


# ── Band thresholds ────────────────────────────────────────────────────────
def _band(score: float) -> str:
    if score >= 65:
        return "green"
    if score >= 40:
        return "amber"
    return "red"


def _band_label(score: float) -> str:
    if score >= 65:
        return "Ready"
    if score >= 40:
        return "At Risk"
    return "Critical"


# ── Flood type classification ──────────────────────────────────────────────
def _flood_type(mean_elev: float | None, runoff_t: float | None,
                terrain_class: str | None, trigger_rate: float) -> str:
    """
    Classify dominant flood type for a ward from available signals.
    River   — very low elevation (floodplain/low-lying terrain)
    Pluvial — high runoff multiplier (dense urban impervious surface)
    Backflow — low elevation + historically high trigger rate
    Sustained — moderate everything but high historical trigger frequency
    """
    elev = mean_elev or 220.0
    runoff = runoff_t or 1.0
    tc = (terrain_class or "").lower()

    if elev < 207 or tc == "floodplain":
        if trigger_rate > 0.25:
            return "backflow"
        return "river"
    if runoff >= 2.5 or tc == "ridge":
        return "pluvial"
    if trigger_rate > 0.3:
        return "sustained"
    return "pluvial"


async def compute_readiness_scores(conn: AsyncConnection) -> list[dict]:
    """
    Compute Pre-Monsoon Readiness Score for every ward.
    Uses: ward_elevation, hotspot aggregate stats, dispatch history,
          critical_infrastructure proximity.
    """

    await conn.execute(text("SET LOCAL statement_timeout = '25000'"))

    # ── 1. Ward list with elevation ──────────────────────────────────
    wards_raw = await conn.execute(text("""
        SELECT
            w.id, w.name, w.zone_name, w.ward_no,
            COALESCE(we.mean_elevation, 220.0)  AS mean_elevation,
            COALESCE(we.runoff_t,       1.5)    AS runoff_t,
            COALESCE(we.mean_slope,     2.0)    AS mean_slope,
            COALESCE(we.terrain_class, 'urban') AS terrain_class
        FROM wards w
        LEFT JOIN ward_elevation we ON we.ward_id = w.id
        ORDER BY w.id
    """))
    wards = wards_raw.mappings().fetchall()

    if not wards:
        return []

    # ── 2. Hotspot capacity averages per ward ────────────────────────
    cap_raw = await conn.execute(text("""
        SELECT
            ward_id,
            AVG(capacity_c)           AS avg_capacity,
            AVG(critical_penalty_pc)  AS avg_penalty,
            COUNT(*)                  AS hotspot_count
        FROM hotspots
        GROUP BY ward_id
    """))
    cap_map = {r["ward_id"]: dict(r) for r in cap_raw.mappings().fetchall()}

    # ── 3. Historical trigger rate from dispatch_runs ────────────────
    hist_raw = await conn.execute(text("""
        SELECT
            ward_id,
            COUNT(*)                                                  AS total_runs,
            COUNT(*) FILTER (WHERE status IN ('critical','dispatched')) AS triggered_runs,
            MIN(ws_score)                                             AS worst_score
        FROM dispatch_runs
        GROUP BY ward_id
    """))
    hist_map = {r["ward_id"]: dict(r) for r in hist_raw.mappings().fetchall()}

    # ── 4. Critical infra exposure per ward ─────────────────────────
    infra_raw = await conn.execute(text("""
        SELECT w.id AS ward_id, COUNT(ci.id) AS infra_count
        FROM wards w
        LEFT JOIN critical_infrastructure ci
          ON ST_DWithin(
              ST_SetSRID(ST_MakePoint(
                  ST_X(ST_Centroid(w.geom)),
                  ST_Y(ST_Centroid(w.geom))
              ), 4326)::geography,
              ci.geom::geography,
              2000
          )
        GROUP BY w.id
    """))
    infra_map = {r["ward_id"]: r["infra_count"] for r in infra_raw.mappings().fetchall()}

    # ── 5. Normalisation ranges ──────────────────────────────────────
    # Collect raw values first, then normalise 0–1
    raw = []
    for w in wards:
        wid = w["id"]
        cap_info   = cap_map.get(wid, {})
        hist_info  = hist_map.get(wid, {})
        infra_cnt  = infra_map.get(wid, 0)

        total_runs    = hist_info.get("total_runs", 0) or 0
        triggered     = hist_info.get("triggered_runs", 0) or 0
        trigger_rate  = triggered / total_runs if total_runs > 0 else 0.0
        avg_cap       = float(cap_info.get("avg_capacity", 50.0) or 50.0)
        avg_penalty   = float(cap_info.get("avg_penalty", 0.0) or 0.0)
        runoff_t      = float(w["runoff_t"])
        mean_elev     = float(w["mean_elevation"])

        # Elevation risk: wards below 210m (Yamuna danger zone) are higher risk
        elev_risk = max(0.0, min(1.0, (215.0 - mean_elev) / 20.0))

        raw.append({
            "ward_id":      wid,
            "name":         w["name"],
            "zone_name":    w["zone_name"],
            "ward_no":      w["ward_no"],
            "mean_elevation": mean_elev,
            "runoff_t":     runoff_t,
            "terrain_class": w["terrain_class"],
            "trigger_rate": trigger_rate,
            "avg_capacity": avg_cap,
            "avg_penalty":  avg_penalty,
            "elev_risk":    elev_risk,
            "infra_count":  infra_cnt,
            "total_runs":   total_runs,
            "triggered_runs": triggered,
        })

    # Normalise each dimension across all wards
    def _norm(vals: list[float]) -> list[float]:
        mn, mx = min(vals), max(vals)
        if mx == mn:
            return [0.5] * len(vals)
        return [(v - mn) / (mx - mn) for v in vals]

    tr_norm   = _norm([r["trigger_rate"] for r in raw])
    cap_norm  = _norm([r["avg_capacity"]  for r in raw])
    er_norm   = [r["elev_risk"] for r in raw]   # already 0–1
    ic_norm   = _norm([float(r["infra_count"]) for r in raw])

    results = []
    for i, r in enumerate(raw):
        flood_risk_n  = tr_norm[i]
        capacity_n    = cap_norm[i]       # higher cap = better
        vuln_n        = er_norm[i]
        infra_exp_n   = ic_norm[i]

        score = (
            100.0
            - 40.0 * flood_risk_n
            - 30.0 * (1.0 - capacity_n)
            - 20.0 * vuln_n
            - 10.0 * infra_exp_n
        )
        score = round(max(0.0, min(100.0, score)), 1)

        flood_type = _flood_type(
            r["mean_elevation"], r["runoff_t"],
            r["terrain_class"],  r["trigger_rate"]
        )

        results.append({
            "ward_id":        r["ward_id"],
            "ward_name":      r["name"],
            "zone_name":      r["zone_name"] or "—",
            "ward_no":        r["ward_no"] or "—",
            "readiness_score": score,
            "band":           _band(score),
            "band_label":     _band_label(score),
            "flood_type":     flood_type,
            "factors": {
                "flood_risk_pct":   round(flood_risk_n  * 100, 1),
                "capacity_pct":     round(capacity_n    * 100, 1),
                "elevation_risk_pct": round(vuln_n      * 100, 1),
                "infra_exposure_pct": round(infra_exp_n * 100, 1),
            },
            "terrain_class":   r["terrain_class"],
            "mean_elevation":  round(r["mean_elevation"], 1),
            "trigger_rate_pct": round(r["trigger_rate"] * 100, 1),
            "times_triggered": r["triggered_runs"],
        })

    # Sort: red first, then amber, then green; within band by score asc
    band_order = {"red": 0, "amber": 1, "green": 2}
    results.sort(key=lambda x: (band_order[x["band"]], x["readiness_score"]))

    return results


async def readiness_summary(scores: list[dict]) -> dict:
    """Aggregate summary stats from a list of readiness score dicts."""
    if not scores:
        return {}
    red   = sum(1 for s in scores if s["band"] == "red")
    amber = sum(1 for s in scores if s["band"] == "amber")
    green = sum(1 for s in scores if s["band"] == "green")
    avg   = round(sum(s["readiness_score"] for s in scores) / len(scores), 1)

    from datetime import date
    days_to_monsoon = (date(datetime.now().year, 6, 15) - date.today()).days
    days_to_monsoon = max(0, days_to_monsoon)

    flood_type_counts: dict[str, int] = {}
    for s in scores:
        ft = s["flood_type"]
        flood_type_counts[ft] = flood_type_counts.get(ft, 0) + 1

    return {
        "total_wards":       len(scores),
        "red_wards":         red,
        "amber_wards":       amber,
        "green_wards":       green,
        "avg_readiness":     avg,
        "days_to_monsoon":   days_to_monsoon,
        "flood_type_counts": flood_type_counts,
        "computed_at":       datetime.utcnow().isoformat(),
    }
