"""
PMRS v3 scoring engine — three-mechanism flood model, pure calculation, no DB writes.

Grounded in IIT Delhi Drainage Master Plan for NCT Delhi (2018):
  • 3 basin model: Najafgarh, Barapullah, Trans-Yamuna
  • SWMM junction-flooding as failure metric (volume m³ + duration)
  • Horton infiltration: f∞=0.635 mm/hr (Group D loam), f₀=76.2 mm/hr (dry loam)
  • Manning's n: 0.014 (impervious), 0.012 (RCC box), 0.025 (open drain)
  • Depression storage: 1.59 mm (impervious), 6.35 mm (pervious)
  • IDF design storm: 2-year return period at Safdarjung/Palam

Three flood mechanisms (IIT Delhi Ch. 2-3):
  1. PLUVIAL  — rainfall exceeds drain conveyance capacity
     Key: rainfall intensity vs drain capacity (SWMM conduit flow)
  2. FLUVIAL  — Yamuna/drain overflow submerges outfalls
     Key: river stage, elevation, outfall submergence
  3. COMPOUND — simultaneous rain + high river → drain backflow
     Key: interaction term (rain × river penalty × low elevation)

Formula
-------
  pluvial_score  = MEAN(min(100, C_eff_i / (R_peak_i × T_i) × PLUVIAL_SCALE))
  fluvial_score  = 100 - yamuna_penalty - terrain_penalty
  compound_score = 100 - backflow_interaction

  W_s = 0.45 × pluvial_score
      + 0.30 × fluvial_score
      + 0.25 × compound_score

  Weights rationale (IIT Delhi exec summary + CWC 2023 case study):
    • 45% pluvial: most frequent mechanism; 3854 flooded junctions in Najafgarh
      alone under 2-yr storm (IIT Delhi Scenario 1, Table 3.3-3)
    • 30% fluvial: Yamuna 208.66m in 2023 flooded all Trans-Yamuna; outfall
      submergence identified as second-largest amplifier (CWC 2023)
    • 25% compound: IIT Delhi shows drain backflow when outfalls submerged;
      even 20 mm/hr causes severe flooding under compound conditions

Calibration (PLUVIAL_SCALE = 18.5):
  C=100, R=12 mm/hr (2-yr design storm), T=1.0  → pluvial=100 (safe)
  C=100, R=33.6 (cloudburst),            T=1.0  → pluvial= 55 (triggered)
  C=100, R=33.6, T=3.0 (Trans-Yamuna)           → pluvial= 18 (critical)
  C=35,  R=33.6, T=1.0 (silted drain)           → pluvial= 19 (critical)

Compound drain surcharge (new):
  Cloudburst, 213m elev, Yamuna NORMAL → compound≈87, ws≈55 (yellow)
  Cloudburst, 210m elev, Yamuna NORMAL → compound≈68, ws≈50 (amber)
  Cloudburst, 207m floodplain, NORMAL  → compound≈51, ws≈39 (red)
  Cloudburst + Yamuna DANGER, 207m     → compound≈14, ws≈18 (critical)

rain_norm denominator = 12 mm/hr (IIT Delhi 2-yr design storm at Safdarjung/Palam).
  At or above the design storm, compound interaction is fully weighted.
  At 6 mm/hr (heavy but sub-design), rain_norm = 0.5 — partial compound stress.
  Saturates at 12 mm/hr: a sustained design-storm event fully activates compound.

Data sources:
  IIT Delhi DMP 2018 (Ch 4): Manning's n Table 4.1-3, Horton Table 4.3-1/2/3
  CWC 2023 Delhi flood case study: Yamuna thresholds
  SRTM v4.1: ward elevation + terrain classification
  Open-Meteo: real-time rainfall intensity
"""

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


# ── Calibration constants ─────────────────────────────────────────────────────
# Grounded in IIT Delhi DMP 2018 SWMM simulations

PLUVIAL_SCALE   = 18.5   # maps C/(R×T) ratio to 0-100 score
RAIN_WINDOW_MIN = 180    # 3-hour accumulation window (IIT Delhi uses 15-min IDF,
                         #   we aggregate over 3h to capture sustained events)
TRIGGER_SCORE   = 65     # ws_score < TRIGGER_SCORE → ward is triggered
CRITICAL_SCALE  = 20.0   # normalises critical_penalty_pc

# ── Mechanism weights (sum = 1.0) ─────────────────────────────────────────────
# Source: IIT Delhi DMP Ch 3 simulation results + CWC 2023 flood analysis
W_PLUVIAL  = 0.45   # rain-on-drain: most common, 3854 junctions flooded (Najafgarh 2-yr)
W_FLUVIAL  = 0.30   # river/outfall: 2023 Yamuna 208.66m flooded Trans-Yamuna entirely
W_COMPOUND = 0.25   # backflow interaction: low rain + high river = severe flooding

# ── Horton infiltration parameters (IIT Delhi DMP Table 4.3-1/2/3) ────────────
# Delhi soil: loam, Hydrologic Group D
HORTON_F_INF    = 0.635   # f∞ mm/hr — equilibrium infiltration (Group D, Table 4.3-1)
HORTON_F_0      = 76.2    # f₀ mm/hr — initial infiltration, dry loam (Table 4.3-2: 3 in/hr)
HORTON_K_DECAY  = 0.00115 # k sec⁻¹ — decay coefficient (Table 4.3-3)

# ── Terrain class → pluvial adjustment (points) ──────────────────────────────
# Source: SRTM v4.1 + IIT Delhi basin delineation + CWC elevation thresholds
# Floodplain wards flood at 2-yr storm; ridge wards drain naturally via slope
TERRAIN_ADJ: dict[str, float] = {
    "floodplain": -15.0,  # Yamuna Khadar — IIT Delhi Trans-Yamuna basin, alluvial clay
    "low":         -8.0,  # Below 207m MSL — drain outfalls partially submerged
    "flat":         0.0,  # Standard urban — Delhi design storm capacity
    "moderate":    +3.0,  # Natural gradient — IIT Delhi slope-aided drainage
    "steep":       +6.0,  # Good slope — rapid runoff clearance
    "ridge":       +8.0,  # Delhi Ridge — highest natural drainage (IIT Delhi Aravalli reference)
}

# ── Yamuna discharge → fluvial penalty (points deducted from 100) ─────────────
# Source: CWC Hathnikund gauge thresholds + IIT Delhi outfall submergence data
# High discharge → river can't accept drain outflow → outfall submergence
YAMUNA_PENALTY: dict[str, float] = {
    "NORMAL":   0.0,    # < 100k cusecs — drains flow freely
    "WATCH":    8.0,    # 100-200k cusecs — partial outfall restriction
    "WARNING": 20.0,    # 200-300k cusecs — significant backwater
    "DANGER":  35.0,    # > 300k cusecs — outfalls submerged (2023: 208.66m stage)
    "EXTREME": 50.0,    # catastrophic — all low-lying outfalls underwater
    "UNKNOWN":  0.0,
}


# ── Main scoring function ─────────────────────────────────────────────────────

async def compute_ward_score(
    ward_id: int,
    conn: AsyncConnection,
    cutoff_override: datetime | None = None,
    yamuna_status: str = "NORMAL",
) -> dict:
    """
    Compute the three-mechanism PMRS v3 ward score for *ward_id*.

    Three mechanisms scored independently then combined:
      1. Pluvial (rain vs drain capacity) — per-hotspot SWMM-style evaluation
      2. Fluvial (river/outfall submergence) — elevation + Yamuna discharge
      3. Compound (interaction: rain + river) — backflow amplification

    cutoff_override : for backtest — earliest rain event timestamp to include.
    yamuna_status   : current Hathnikund alert level.

    Returns a dict — never writes to the DB.
    """

    # ── Ward metadata + elevation ─────────────────────────────────────────────
    ward_row = await conn.execute(
        text("""
            SELECT w.id, w.name,
                   COALESCE(we.terrain_class, 'flat')   AS terrain_class,
                   COALESCE(we.mean_elevation, 213.0)   AS mean_elevation
            FROM wards w
            LEFT JOIN ward_elevation we ON we.ward_id = w.id
            WHERE w.id = :wid
        """),
        {"wid": ward_id},
    )
    ward = ward_row.mappings().first()
    if ward is None:
        return _unknown_result(ward_id)

    mean_elev     = float(ward["mean_elevation"])
    terrain_class = ward["terrain_class"].lower()

    # ── Quick short-circuit: no hotspots → safe ───────────────────────────────
    total = (await conn.execute(
        text("SELECT COUNT(*) FROM hotspots WHERE ward_id = :wid"),
        {"wid": ward_id},
    )).scalar_one()
    if total == 0:
        return _safe_result(ward_id, ward["name"])

    # ── Rain window ───────────────────────────────────────────────────────────
    if cutoff_override is not None:
        cutoff       = cutoff_override
        cutoff_upper = cutoff_override + timedelta(hours=24)
    else:
        cutoff       = datetime.utcnow() - timedelta(minutes=RAIN_WINDOW_MIN)
        cutoff_upper = datetime.utcnow()

    # ── Hotspots intersecting rain in window ──────────────────────────────────
    rows = await conn.execute(
        text("""
            SELECT
                h.id                            AS hotspot_id,
                h.capacity_c,
                h.runoff_t,
                h.critical_penalty_pc,
                MAX(re.intensity_r)             AS peak_intensity,
                COUNT(DISTINCT re.id)           AS event_count
            FROM hotspots h
            JOIN rain_events re
              ON ST_Intersects(h.geom, re.geom)
             AND re.created_at >= :cutoff
             AND re.created_at <  :cutoff_upper
            WHERE h.ward_id = :wid
            GROUP BY h.id, h.capacity_c, h.runoff_t, h.critical_penalty_pc
        """),
        {"wid": ward_id, "cutoff": cutoff, "cutoff_upper": cutoff_upper},
    )
    hotspots = rows.mappings().fetchall()

    if len(hotspots) == 0:
        return _safe_result(ward_id, ward["name"])

    # ══════════════════════════════════════════════════════════════════════════
    # MECHANISM 1: PLUVIAL SCORE (rain-on-drain)
    # IIT Delhi DMP: SWMM sub-catchment → conduit flow → junction overflow
    # Proxy: capacity / (peak_intensity × runoff_terrain) with Horton saturation
    # ══════════════════════════════════════════════════════════════════════════
    pluvial_scores: list[float] = []
    penalties:      list[float] = []

    for h in hotspots:
        r_peak = max(float(h["peak_intensity"]), 0.1)
        t_i    = max(float(h["runoff_t"]),       1.0)
        c_i    = max(float(h["capacity_c"]),     0.0)

        # Horton soil saturation factor (IIT Delhi DMP Table 4.3-1/2/3):
        # Multiple rain events → soil approaches equilibrium infiltration (f∞).
        # Each additional event reduces effective capacity by up to 20%.
        # This proxies the SWMM Horton model: f_p = f∞ + (f₀ - f∞)·e^(-kt)
        event_count = int(h["event_count"])
        sat_factor  = max(0.80, 1.0 - (event_count - 1) * 0.05)

        raw     = (c_i * sat_factor) / (r_peak * t_i)
        score_i = min(100.0, raw * PLUVIAL_SCALE)
        pluvial_scores.append(score_i)
        penalties.append(float(h["critical_penalty_pc"]))

    n = len(pluvial_scores)
    mean_pluvial = sum(pluvial_scores) / n
    mean_penalty = (sum(penalties) / n) / CRITICAL_SCALE

    # Terrain adjustment for pluvial component
    terrain_adj = TERRAIN_ADJ.get(terrain_class, 0.0)
    pluvial_final = max(0.0, min(100.0, mean_pluvial - mean_penalty + terrain_adj))

    # ══════════════════════════════════════════════════════════════════════════
    # MECHANISM 2: FLUVIAL SCORE (river/outfall submergence)
    # IIT Delhi DMP: outfalls into Yamuna are the final disposal points;
    # when river stage rises, outfalls submerge → drains can't discharge.
    # CWC 2023: Yamuna 208.66m submerged all Trans-Yamuna outfalls.
    # Proxy: base 100 minus elevation-weighted Yamuna discharge penalty.
    # ══════════════════════════════════════════════════════════════════════════
    raw_yamuna_pen = YAMUNA_PENALTY.get(yamuna_status.upper(), 0.0)

    # Elevation-weighted penalty: full effect at ≤207m (floodplain),
    # zero at ≥215m (upland), linear taper between.
    # Source: CWC danger level 205.33m + IIT Delhi basin elevation data.
    if mean_elev >= 215.0:
        elev_yamuna_pen = 0.0
    elif mean_elev <= 207.0:
        elev_yamuna_pen = raw_yamuna_pen
    else:
        elev_yamuna_pen = raw_yamuna_pen * (215.0 - mean_elev) / 8.0

    # Terrain-based fluvial vulnerability
    # Floodplain/low wards have additional structural vulnerability
    # (IIT Delhi: Trans-Yamuna is former floodplain, alluvial clay, poor drainage)
    terrain_fluvial_pen = {
        "floodplain": 20.0,
        "low":        10.0,
        "flat":        0.0,
        "moderate":    0.0,
        "steep":       0.0,
        "ridge":       0.0,
    }.get(terrain_class, 0.0)

    fluvial_final = max(0.0, min(100.0, 100.0 - elev_yamuna_pen - terrain_fluvial_pen))

    # ══════════════════════════════════════════════════════════════════════════
    # MECHANISM 3: COMPOUND SCORE (drain surcharge + river backflow)
    # IIT Delhi DMP: "storm water has to be managed so it does not interfere
    # with drainage" — junction overflow occurs when drain capacity is
    # overwhelmed (surcharging), amplified when outfalls are submerged.
    # Two compound components:
    #   a) Drain surcharge: pluvial failure → water backs up at junctions
    #      (IIT Delhi: 3854 junctions flooded in Najafgarh, 2-yr storm)
    #   b) River backflow: Yamuna high + drain failure → outfall submergence
    #      (CWC 2023: 208.66m stage submerged all Trans-Yamuna outfalls)
    # ══════════════════════════════════════════════════════════════════════════
    max_rain = max(float(h["peak_intensity"]) for h in hotspots)
    # rain_norm: saturates at 12 mm/hr (IIT Delhi 2-yr design storm).
    # At the design storm the compound interaction is fully activated;
    # sub-design events get proportional weight.
    rain_norm  = min(1.0, max_rain / 12.0)
    river_norm = min(1.0, raw_yamuna_pen / 50.0)
    elev_vuln  = max(0.0, min(1.0, (215.0 - mean_elev) / 8.0))

    # Pluvial system stress: how badly drains are failing (0=safe, 1=total failure)
    pluvial_stress = max(0.0, 1.0 - pluvial_final / 100.0)

    # (a) Drain surcharge → junction overflow (primary compound mechanism)
    # When rainfall overwhelms drain capacity, water backs up at junctions.
    # Elevation modulates: low wards flood first, but all affected wards suffer.
    drain_surcharge_pen = pluvial_stress * rain_norm * (0.3 + 0.7 * elev_vuln) * 60.0

    # (b) River × drain backflow: high river AND failing drains → outfall submergence
    backflow_pen = pluvial_stress * river_norm * elev_vuln * 60.0

    compound_penalty = drain_surcharge_pen + backflow_pen
    compound_final = max(0.0, min(100.0, 100.0 - compound_penalty))

    # ══════════════════════════════════════════════════════════════════════════
    # COMBINED SCORE — weighted sum of three mechanisms
    # ══════════════════════════════════════════════════════════════════════════
    ws_score = (
        W_PLUVIAL  * pluvial_final
        + W_FLUVIAL  * fluvial_final
        + W_COMPOUND * compound_final
    )
    ws_score = round(max(ws_score, 0.0), 2)

    return {
        "ward_id":          ward_id,
        "ward_name":        ward["name"],
        "ws_score":         ws_score,
        "hotspots_in_rain": n,
        "triggered":        ws_score < TRIGGER_SCORE,
        "computed_at":      datetime.utcnow(),
        # Mechanism breakdown for frontend/API consumers
        "mechanisms": {
            "pluvial":  round(pluvial_final, 1),
            "fluvial":  round(fluvial_final, 1),
            "compound": round(compound_final, 1),
        },
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_result(ward_id: int, ward_name: str) -> dict:
    """Ward is safe — no rain overlap or no hotspots."""
    return {
        "ward_id":         ward_id,
        "ward_name":       ward_name,
        "ws_score":        100.0,
        "hotspots_in_rain": 0,
        "triggered":       False,
        "computed_at":     datetime.utcnow(),
    }


def _unknown_result(ward_id: int) -> dict:
    return {
        "ward_id":         ward_id,
        "ward_name":       "UNKNOWN",
        "ws_score":        100.0,
        "hotspots_in_rain": 0,
        "triggered":       False,
        "computed_at":     datetime.utcnow(),
    }


# ── Batch scoring (used by run_cycle_internal) ────────────────────────────────

async def score_all_wards_batch(
    conn: AsyncConnection,
    yamuna_status: str = "NORMAL",
    cutoff_override: datetime | None = None,
) -> list[dict]:
    """
    Score ALL wards in 3 SQL queries instead of 870+ sequential ones.

    Query 1: all wards + elevation metadata
    Query 2: hotspot counts per ward (for short-circuit)
    Query 3: hotspots × rain_events spatial join — ALL wards at once

    Then score each ward in pure Python (no further DB calls).
    """
    if cutoff_override is not None:
        cutoff       = cutoff_override
        cutoff_upper = cutoff_override + timedelta(hours=24)
    else:
        now = datetime.utcnow()
        cutoff       = now - timedelta(minutes=RAIN_WINDOW_MIN)
        cutoff_upper = now

    # ── Query 1: ward metadata ────────────────────────────────────────────
    ward_rows = (await conn.execute(text("""
        SELECT w.id, w.name,
               COALESCE(we.terrain_class, 'flat')   AS terrain_class,
               COALESCE(we.mean_elevation, 213.0)   AS mean_elevation
        FROM wards w
        LEFT JOIN ward_elevation we ON we.ward_id = w.id
        ORDER BY w.id
    """))).mappings().fetchall()

    if not ward_rows:
        return []

    # ── Query 2: hotspot counts per ward ──────────────────────────────────
    count_rows = (await conn.execute(text(
        "SELECT ward_id, COUNT(*) AS cnt FROM hotspots GROUP BY ward_id"
    ))).mappings().fetchall()
    hotspot_counts = {r["ward_id"]: r["cnt"] for r in count_rows}

    # ── Query 3: ALL hotspots intersecting rain — single spatial join ─────
    rain_rows = (await conn.execute(text("""
        SELECT
            h.ward_id,
            h.id                            AS hotspot_id,
            h.capacity_c,
            h.runoff_t,
            h.critical_penalty_pc,
            MAX(re.intensity_r)             AS peak_intensity,
            COUNT(DISTINCT re.id)           AS event_count
        FROM hotspots h
        JOIN rain_events re
          ON ST_Intersects(h.geom, re.geom)
         AND re.created_at >= :cutoff
         AND re.created_at <  :cutoff_upper
        GROUP BY h.ward_id, h.id, h.capacity_c, h.runoff_t, h.critical_penalty_pc
    """), {"cutoff": cutoff, "cutoff_upper": cutoff_upper})).mappings().fetchall()

    # Group rain-affected hotspots by ward_id
    from collections import defaultdict
    ward_hotspots: dict[int, list] = defaultdict(list)
    for r in rain_rows:
        ward_hotspots[r["ward_id"]].append(r)

    # ── Score each ward in Python ─────────────────────────────────────────
    results = []
    for w in ward_rows:
        wid = w["id"]

        # No hotspots at all → safe
        if hotspot_counts.get(wid, 0) == 0:
            results.append(_safe_result(wid, w["name"]))
            continue

        # No rain overlap → safe
        hotspots = ward_hotspots.get(wid)
        if not hotspots:
            results.append(_safe_result(wid, w["name"]))
            continue

        # Score using same three-mechanism logic
        mean_elev     = float(w["mean_elevation"])
        terrain_class = w["terrain_class"].lower()

        # ── PLUVIAL ───────────────────────────────────────────────────────
        pluvial_scores = []
        penalties      = []
        for h in hotspots:
            r_peak = max(float(h["peak_intensity"]), 0.1)
            t_i    = max(float(h["runoff_t"]),       1.0)
            c_i    = max(float(h["capacity_c"]),     0.0)
            event_count = int(h["event_count"])
            sat_factor  = max(0.80, 1.0 - (event_count - 1) * 0.05)
            raw     = (c_i * sat_factor) / (r_peak * t_i)
            score_i = min(100.0, raw * PLUVIAL_SCALE)
            pluvial_scores.append(score_i)
            penalties.append(float(h["critical_penalty_pc"]))

        n = len(pluvial_scores)
        mean_pluvial = sum(pluvial_scores) / n
        mean_penalty = (sum(penalties) / n) / CRITICAL_SCALE
        terrain_adj  = TERRAIN_ADJ.get(terrain_class, 0.0)
        pluvial_final = max(0.0, min(100.0, mean_pluvial - mean_penalty + terrain_adj))

        # ── FLUVIAL ───────────────────────────────────────────────────────
        raw_yamuna_pen = YAMUNA_PENALTY.get(yamuna_status.upper(), 0.0)
        if mean_elev >= 215.0:
            elev_yamuna_pen = 0.0
        elif mean_elev <= 207.0:
            elev_yamuna_pen = raw_yamuna_pen
        else:
            elev_yamuna_pen = raw_yamuna_pen * (215.0 - mean_elev) / 8.0

        terrain_fluvial_pen = {
            "floodplain": 20.0, "low": 10.0, "flat": 0.0,
            "moderate": 0.0, "steep": 0.0, "ridge": 0.0,
        }.get(terrain_class, 0.0)
        fluvial_final = max(0.0, min(100.0, 100.0 - elev_yamuna_pen - terrain_fluvial_pen))

        # ── COMPOUND (drain surcharge + river backflow) ────────────────
        max_rain   = max(float(h["peak_intensity"]) for h in hotspots)
        rain_norm  = min(1.0, max_rain / 12.0)   # saturates at 2-yr design storm
        river_norm = min(1.0, raw_yamuna_pen / 50.0)
        elev_vuln  = max(0.0, min(1.0, (215.0 - mean_elev) / 8.0))

        pluvial_stress = max(0.0, 1.0 - pluvial_final / 100.0)
        drain_surcharge_pen = pluvial_stress * rain_norm * (0.3 + 0.7 * elev_vuln) * 60.0
        backflow_pen = pluvial_stress * river_norm * elev_vuln * 60.0
        compound_penalty = drain_surcharge_pen + backflow_pen
        compound_final = max(0.0, min(100.0, 100.0 - compound_penalty))

        # ── COMBINED ──────────────────────────────────────────────────────
        ws_score = (
            W_PLUVIAL  * pluvial_final
            + W_FLUVIAL  * fluvial_final
            + W_COMPOUND * compound_final
        )
        ws_score = round(max(ws_score, 0.0), 2)

        results.append({
            "ward_id":          wid,
            "ward_name":        w["name"],
            "ws_score":         ws_score,
            "hotspots_in_rain": n,
            "triggered":        ws_score < TRIGGER_SCORE,
            "computed_at":      now,
            "mechanisms": {
                "pluvial":  round(pluvial_final, 1),
                "fluvial":  round(fluvial_final, 1),
                "compound": round(compound_final, 1),
            },
        })

    return results
