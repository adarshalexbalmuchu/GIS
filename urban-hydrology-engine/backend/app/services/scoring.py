"""
PMRS scoring engine — pure calculation, no DB writes.

W_s = SUM(C_i / (R_i * T_i)) - SUM(critical_penalty_pc_i)
"""

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def compute_ward_score(ward_id: int, conn: AsyncConnection) -> dict:
    """
    Compute the PMRS ward score for *ward_id* using rain events
    from the last 60 minutes.

    Returns a dict — never writes to the DB.
    """

    # ── Ward metadata ────────────────────────────────────────────────
    ward_row = await conn.execute(
        text("SELECT id, name FROM wards WHERE id = :wid"),
        {"wid": ward_id},
    )
    ward = ward_row.mappings().first()
    if ward is None:
        return {
            "ward_id": ward_id,
            "ward_name": "UNKNOWN",
            "ws_score": 100.0,
            "hotspots_in_rain": 0,
            "triggered": False,
            "computed_at": datetime.utcnow(),
        }

    # ── Count hotspots in ward (quick short-circuit) ─────────────────
    total = await conn.execute(
        text("SELECT COUNT(*) FROM hotspots WHERE ward_id = :wid"),
        {"wid": ward_id},
    )
    if total.scalar_one() == 0:
        return _safe_result(ward_id, ward["name"])

    # ── Hotspots that intersect recent rain ──────────────────────────
    cutoff = datetime.utcnow() - timedelta(minutes=60)

    rows = await conn.execute(
        text("""
            SELECT
                h.id            AS hotspot_id,
                h.capacity_c,
                h.runoff_t,
                h.critical_penalty_pc,
                MAX(re.intensity_r) AS max_intensity
            FROM hotspots h
            JOIN rain_events re
              ON ST_Intersects(h.geom, re.geom)
             AND re.created_at >= :cutoff
            WHERE h.ward_id = :wid
            GROUP BY h.id, h.capacity_c, h.runoff_t, h.critical_penalty_pc
        """),
        {"wid": ward_id, "cutoff": cutoff},
    )
    hotspots = rows.mappings().fetchall()

    if len(hotspots) == 0:
        return _safe_result(ward_id, ward["name"])

    # ── PMRS formula ─────────────────────────────────────────────────
    term_sum = 0.0
    penalty_sum = 0.0

    for h in hotspots:
        r_i = max(h["max_intensity"], 0.1)   # division-by-zero guard
        t_i = max(h["runoff_t"], 1.0)         # floor at 1.0
        c_i = h["capacity_c"]
        term_sum += c_i / (r_i * t_i)
        penalty_sum += h["critical_penalty_pc"]

    ws_score = round(term_sum - penalty_sum, 2)
    ws_score = max(ws_score, -999.0)

    return {
        "ward_id": ward_id,
        "ward_name": ward["name"],
        "ws_score": ws_score,
        "hotspots_in_rain": len(hotspots),
        "triggered": ws_score < 70,
        "computed_at": datetime.utcnow(),
    }


# ── helper ───────────────────────────────────────────────────────────
def _safe_result(ward_id: int, ward_name: str) -> dict:
    """Ward is safe — no rain overlap or no hotspots."""
    return {
        "ward_id": ward_id,
        "ward_name": ward_name,
        "ws_score": 100.0,
        "hotspots_in_rain": 0,
        "triggered": False,
        "computed_at": datetime.utcnow(),
    }
