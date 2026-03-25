"""
PMRS v2 scoring engine — pure calculation, no DB writes.

Formula
-------
W_s = MEAN(min(100, C_i / (R_acc_i × T_i) × PMRS_SCALE))
      + terrain_adj
      - yamuna_penalty
      - MEAN(critical_penalty_pc_i) / CRITICAL_SCALE

Scale calibration (PMRS_SCALE = 18.5):
  C=100, R=12 mm/hr (Delhi design storm), T=1.0  →  raw=8.33  →  score=100 (capped, safe)
  C=100, R=33.6 mm/hr (cloudburst Sep-2024),T=1.0 →  raw=2.976 →  score= 55 (triggered)
  C=100, R=33.6, T=3.0 (Yamuna Khader terrain)   →  raw=0.992 →  score= 18 (critical)
  C=35,  R=33.6, T=1.0 (degraded capacity)        →  raw=1.042 →  score= 19 (critical)

Key changes vs v1:
  • MEAN not SUM  — removes "more hotspots = safer" artefact
  • 3-hour window — captures full cloudburst accumulation
  • SUM(intensity) — accumulated rain load, not just peak event
  • Terrain adj    — floodplain/low wards penalised; ridge wards get slight bonus
  • Yamuna penalty — backwater effect reduces effective drainage when river is high
  • Critical infra  — normalised mean penalty (÷ CRITICAL_SCALE) instead of raw sum
  • Trigger @ 65   — recalibrated to new scale
"""

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


# ── Calibration constants ─────────────────────────────────────────────────────

PMRS_SCALE      = 18.5   # see docstring
RAIN_WINDOW_MIN = 180    # 3-hour accumulation window
TRIGGER_SCORE   = 65     # ws_score < TRIGGER_SCORE → ward is triggered
CRITICAL_SCALE  = 20.0   # divides mean critical_penalty_pc before subtracting

# Terrain class → score adjustment (points added/subtracted from mean hotspot score)
# Source: GNCTD flood maps + CWC elevation thresholds for Delhi wards
TERRAIN_ADJ: dict[str, float] = {
    "floodplain": -15.0,  # Yamuna Khadar — riverbed, floods every high-monsoon year
    "low":         -8.0,  # Below 207m MSL — backs up when Yamuna is in warning
    "flat":         0.0,  # Standard Delhi drainage, no adjustment
    "moderate":    +3.0,  # Natural gradient aids drainage
    "steep":       +6.0,  # Good slope, water moves away quickly
    "ridge":       +8.0,  # Delhi Ridge / elevated wards, best natural drainage
}

# Yamuna Hathnikund discharge → backwater penalty (points deducted)
# High discharge means the river can't accept drain outflow → wards back-flood
YAMUNA_PENALTY: dict[str, float] = {
    "NORMAL":   0.0,
    "WATCH":    5.0,
    "WARNING": 12.0,
    "DANGER":  20.0,
    "EXTREME": 28.0,
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
    Compute the PMRS v2 ward score for *ward_id*.

    cutoff_override : for backtest — earliest rain event timestamp to include.
    yamuna_status   : current Hathnikund alert level (NORMAL/WATCH/WARNING/DANGER/EXTREME).

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
    # SUM(intensity_r) = accumulated rain load over the window, not just peak event.
    # This correctly penalises sustained heavy rain vs a single brief pulse.
    rows = await conn.execute(
        text("""
            SELECT
                h.id                        AS hotspot_id,
                h.capacity_c,
                h.runoff_t,
                h.critical_penalty_pc,
                SUM(re.intensity_r)         AS acc_intensity
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

    # ── PMRS v2 formula ───────────────────────────────────────────────────────
    scores:   list[float] = []
    penalties: list[float] = []

    for h in hotspots:
        r_acc = max(float(h["acc_intensity"]), 0.1)   # accumulated intensity (guard ÷0)
        t_i   = max(float(h["runoff_t"]),      1.0)   # terrain runoff factor (floor 1.0)
        c_i   = max(float(h["capacity_c"]),    0.0)   # current drainage capacity

        raw     = c_i / (r_acc * t_i)
        score_i = min(100.0, raw * PMRS_SCALE)        # cap at 100 per hotspot
        scores.append(score_i)
        penalties.append(float(h["critical_penalty_pc"]))

    n = len(scores)
    mean_score   = sum(scores) / n
    mean_penalty = (sum(penalties) / n) / CRITICAL_SCALE

    # Terrain adjustment (floodplain = large deduction; ridge = small bonus)
    terrain_adj = TERRAIN_ADJ.get(ward["terrain_class"].lower(), 0.0)

    # Yamuna backwater: high river level prevents drain outflow → reduces score
    yamuna_pen = YAMUNA_PENALTY.get(yamuna_status.upper(), 0.0)

    ws_score = round(mean_score - mean_penalty + terrain_adj - yamuna_pen, 2)
    ws_score = max(ws_score, -999.0)

    return {
        "ward_id":         ward_id,
        "ward_name":       ward["name"],
        "ws_score":        ws_score,
        "hotspots_in_rain": n,
        "triggered":       ws_score < TRIGGER_SCORE,
        "computed_at":     datetime.utcnow(),
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
