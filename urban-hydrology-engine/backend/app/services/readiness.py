"""
Pre-Monsoon Readiness Score (PMRS-Static) engine — three-mechanism model.

Grounded in IIT Delhi Drainage Master Plan for NCT Delhi (2018):
  • 3 basin SWMM model: Najafgarh, Barapullah, Trans-Yamuna
  • 5 simulation scenarios: base → cross-section fix → water body → parks → LID
  • Junction flooding (volume m³ + duration) as primary failure metric
  • Horton infiltration, Manning's n, IDF design storms

Unlike the real-time PMRS (which scores wards *during* rainfall),
the Readiness Score is a *pre-event* static index computed from
infrastructure state + historical patterns — no live rain events.

Three mechanisms scored independently then combined:
  1. PLUVIAL readiness (45%) — drain capacity vs design storm demand
     IIT Delhi: drain conveyance capacity evaluated via SWMM conduit flow
     Proxy: avg hotspot capacity + historical trigger frequency
  2. FLUVIAL readiness (30%) — vulnerability to river/outfall flooding
     IIT Delhi: outfalls into Yamuna are final disposal; elevation determines risk
     Proxy: elevation + terrain class + proximity to Yamuna corridor
  3. COMPOUND readiness (25%) — vulnerability to simultaneous rain + river
     IIT Delhi: drain backflow when outfalls submerged even at moderate rain
     Proxy: interaction of pluvial vulnerability × fluvial vulnerability

Formula:
  pluvial_r  = 100 - 40×flood_risk - 30×(1-capacity) - infra_penalty
  fluvial_r  = f(elevation, terrain_class, Yamuna proximity)
  compound_r = f(pluvial_vulnerability × fluvial_vulnerability)
  PMRS_static = 0.45×pluvial_r + 0.30×fluvial_r + 0.25×compound_r

Data sources:
  IIT Delhi DMP 2018 (Ch 2-4): SWMM methodology, Manning's n, IDF
  CWC 2023 Delhi flood case study: Yamuna thresholds, compound flooding
  SRTM v4.1: ward elevation + terrain classification
  OSM Overpass: critical infrastructure (hospitals, fire stations, substations)
"""

from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


# ── Mechanism weights (match real-time scoring) ────────────────────────────
W_PLUVIAL  = 0.45
W_FLUVIAL  = 0.30
W_COMPOUND = 0.25


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
# Ward name fragments that identify Yamuna-corridor / floodplain wards.
# Used when ward_elevation table is unpopulated (elevation = 220 default).
_RIVER_WARD_FRAGMENTS = [
    "yamuna", "khadar", "wazirabad", "burari", "usmanpur",
    "gokulpuri", "mustafabad", "karawal", "bhajanpura",
    "civil lines", "kashmere", "chandni chowk",
    "geeta colony", "shahdara", "mayur vihar", "patparganj",
    "ito", "rajghat", "mori gate",
]


def _flood_type(mean_elev: float | None, runoff_t: float | None,
                terrain_class: str | None, trigger_rate: float,
                ward_name: str = "") -> str:
    """
    Classify dominant flood type for a ward from available signals.
    River    — Yamuna floodplain wards (elevation or name-based)
    Backflow — same low-lying wards with notable trigger history
    Sustained — higher-elevation wards with frequent trigger history
    Pluvial  — default urban rainfall flooding

    Falls back to name-based classification when ward_elevation is empty
    (mean_elev will be 220.0 from COALESCE default).
    """
    elev = mean_elev if mean_elev is not None else 220.0
    tc = (terrain_class or "").lower()
    name_lower = ward_name.lower()
    rt = runoff_t if runoff_t is not None else 2.0

    # Yamuna-corridor ward — by elevation, terrain class, or name fragment
    is_floodplain = (
        elev < 215                        # raised from 210 → catches transition zone
        or tc == "floodplain"
        or any(frag in name_lower for frag in _RIVER_WARD_FRAGMENTS)
    )

    if is_floodplain:
        # Backflow: floodplain + high runoff multiplier (dense urban over alluvial clay)
        # Replaces trigger_rate threshold — works without historical cycle data
        if rt >= 2.8 or trigger_rate > 0.1:
            return "backflow"
        return "river"

    # Sustained: high runoff multiplier in non-floodplain zone
    # Captures dense urban areas that flood repeatedly from accumulated rain
    if rt >= 3.0 or trigger_rate > 0.15:
        return "sustained"

    # Pluvial — default urban flash flood
    return "pluvial"


async def compute_readiness_scores(conn: AsyncConnection) -> list[dict]:
    """
    Compute Pre-Monsoon Readiness Score for every ward using three-mechanism model.

    Mechanism 1 (Pluvial): drain capacity vs historical flood demand
    Mechanism 2 (Fluvial): elevation + terrain vulnerability to river/outfall flooding
    Mechanism 3 (Compound): interaction vulnerability (rain × river × low-lying)

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

    # ── 5. Build raw feature vectors ─────────────────────────────────
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
            "infra_count":  infra_cnt,
            "total_runs":   total_runs,
            "triggered_runs": triggered,
        })

    # ── 6. Normalise across wards ────────────────────────────────────
    def _norm(vals: list[float]) -> list[float]:
        mn, mx = min(vals), max(vals)
        if mx == mn:
            return [0.5] * len(vals)
        return [(v - mn) / (mx - mn) for v in vals]

    tr_norm  = _norm([r["trigger_rate"] for r in raw])
    cap_norm = _norm([r["avg_capacity"]  for r in raw])
    ic_norm  = _norm([float(r["infra_count"]) for r in raw])

    # ── 7. Three-mechanism scoring ───────────────────────────────────
    results = []
    for i, r in enumerate(raw):
        mean_elev     = r["mean_elevation"]
        terrain_class = (r["terrain_class"] or "flat").lower()

        # ── MECHANISM 1: PLUVIAL READINESS (drain capacity vs flood demand) ──
        # IIT Delhi DMP: drain conveyance adequacy is the primary flood driver
        # (3854 flooded junctions in Najafgarh alone, Scenario 1)
        flood_risk_n = tr_norm[i]      # historical trigger frequency (0-1)
        capacity_n   = cap_norm[i]     # drain capacity (higher = better)
        infra_exp_n  = ic_norm[i]      # critical infra at risk

        pluvial_r = (
            100.0
            - 40.0 * flood_risk_n           # historical flood frequency
            - 30.0 * (1.0 - capacity_n)     # drain capacity deficit
            - 10.0 * infra_exp_n            # critical infrastructure exposure
        )
        pluvial_r = max(0.0, min(100.0, pluvial_r))

        # ── MECHANISM 2: FLUVIAL READINESS (river/outfall vulnerability) ─────
        # IIT Delhi DMP: outfalls into Yamuna are final disposal; when river
        # rises, outfalls submerge. CWC 2023: 208.66m flooded Trans-Yamuna.
        # Elevation risk: wards below 210m (Yamuna danger zone) are highest risk
        elev_risk = max(0.0, min(1.0, (215.0 - mean_elev) / 20.0))

        # Terrain structural vulnerability (IIT Delhi basin classification)
        terrain_fluvial_pen = {
            "floodplain": 0.40,   # Trans-Yamuna: alluvial clay, former riverbed
            "low":        0.25,   # Near-river: drain outfalls partially at risk
            "flat":       0.05,   # Standard urban
            "urban":      0.05,
            "moderate":   0.00,
            "steep":      0.00,
            "ridge":      0.00,
        }.get(terrain_class, 0.05)

        fluvial_r = 100.0 - 50.0 * elev_risk - 30.0 * terrain_fluvial_pen
        fluvial_r = max(0.0, min(100.0, fluvial_r))

        # ── MECHANISM 3: COMPOUND READINESS (rain + river interaction) ────────
        # IIT Delhi DMP Exec Summary: both storms AND drain outfall submergence
        # must be managed together. Compound flooding is rare but catastrophic.
        # Vulnerability = pluvial_vulnerability × fluvial_vulnerability
        pluvial_vuln  = 1.0 - (pluvial_r / 100.0)   # 0 = fully ready, 1 = fully vulnerable
        fluvial_vuln  = 1.0 - (fluvial_r / 100.0)

        # Multiplicative interaction: only severe when BOTH mechanisms are vulnerable
        compound_penalty = pluvial_vuln * fluvial_vuln * 70.0

        # High runoff terrain amplifies compound risk (IIT Delhi: alluvial clay
        # in Trans-Yamuna has runoff_t 2.5-3.5, poor infiltration)
        runoff_amp = max(0.0, (r["runoff_t"] - 2.0) / 2.0)  # 0 at T≤2, 0.5 at T=3
        compound_penalty += runoff_amp * 15.0

        compound_r = max(0.0, min(100.0, 100.0 - compound_penalty))

        # ── COMBINED SCORE ────────────────────────────────────────────
        score = W_PLUVIAL * pluvial_r + W_FLUVIAL * fluvial_r + W_COMPOUND * compound_r
        score = round(max(0.0, min(100.0, score)), 1)

        flood_type = _flood_type(
            r["mean_elevation"], r["runoff_t"],
            r["terrain_class"],  r["trigger_rate"],
            ward_name=r["name"],
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
                "flood_risk_pct":     round(flood_risk_n  * 100, 1),
                "capacity_pct":       round(capacity_n    * 100, 1),
                "elevation_risk_pct": round(elev_risk     * 100, 1),
                "infra_exposure_pct": round(infra_exp_n   * 100, 1),
            },
            "mechanisms": {
                "pluvial":  round(pluvial_r, 1),
                "fluvial":  round(fluvial_r, 1),
                "compound": round(compound_r, 1),
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
