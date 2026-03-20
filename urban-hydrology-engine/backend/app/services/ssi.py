"""
System Saturation Index (SSI) — real computation from DB.

The SSI is a 0–100 daily composite score that measures how close Delhi's
drainage and flood system is to saturation. Unlike the real-time PMRS
(which scores during rainfall) or the Readiness Score (which is pre-event
infrastructure state), the SSI tracks *cumulative system stress*.

Formula:
  SSI = 0.40 × rain_component
      + 0.30 × trigger_component
      + 0.20 × critical_component
      + 0.10 × capacity_component

Components:
  rain_component      — 5-day cumulative rainfall normalised (0=dry, 1=saturated)
  trigger_component   — % of wards triggered in last 5 days (running average)
  critical_component  — % of wards critical in last 5 days
  capacity_component  — mean drain capacity degradation across all hotspots

Thresholds:
  SSI < 30   → Normal        (green)
  SSI 30–50  → Elevated      (yellow)
  SSI 50–75  → Watch         (amber)  — pre-position resources
  SSI ≥ 75   → Pre-position  (red)    — full pre-monsoon activation

Caches for 10 minutes (expensive aggregation query).
"""

from datetime import datetime, timedelta, timezone
import time
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

_IST = timezone(timedelta(hours=5, minutes=30))

# ── Thresholds ────────────────────────────────────────────────────────────
SSI_THRESHOLDS = [
    (75, "Pre-position", "#DC2626"),
    (50, "Watch",        "#D97706"),
    (30, "Elevated",     "#CA8A04"),
    (0,  "Normal",       "#16A34A"),
]

_cache: dict = {"result": None, "ts": 0.0}
_CACHE_TTL = 600  # 10 minutes


def _ssi_level(ssi: float) -> tuple[str, str]:
    for threshold, label, colour in SSI_THRESHOLDS:
        if ssi >= threshold:
            return label, colour
    return "Normal", "#16A34A"


async def compute_ssi(conn: AsyncConnection) -> dict:
    """Compute the System Saturation Index from live DB data."""
    now = time.monotonic()
    if _cache["result"] and (now - _cache["ts"]) < _CACHE_TTL:
        return _cache["result"]

    cutoff_5d = datetime.utcnow() - timedelta(days=5)
    cutoff_1d = datetime.utcnow() - timedelta(days=1)

    # ── 1. Rain component: 5-day cumulative rainfall ──────────────────────
    rain_row = await conn.execute(text("""
        SELECT COALESCE(SUM(intensity_r), 0) AS total_intensity,
               COUNT(*)                       AS event_count
        FROM rain_events
        WHERE created_at >= :cutoff
    """), {"cutoff": cutoff_5d})
    rain_data = rain_row.mappings().first()

    total_intensity = float(rain_data["total_intensity"] or 0)
    # Normalise: 0 = no rain, 1 = extreme (intensity sum > 50 = fully saturated)
    rain_component = min(1.0, total_intensity / 50.0)

    # ── 2. Trigger component: % wards triggered in last 5 days ───────────
    trigger_row = await conn.execute(text("""
        SELECT
            COUNT(DISTINCT ward_id)                                           AS total_wards,
            COUNT(DISTINCT ward_id) FILTER (
                WHERE status IN ('triggered','dispatched','critical')
                AND   created_at >= :cutoff
            )                                                                 AS triggered_wards
        FROM dispatch_runs
    """), {"cutoff": cutoff_5d})
    trig_data = trigger_row.mappings().first()

    total_wards    = int(trig_data["total_wards"]    or 1)
    triggered_5d   = int(trig_data["triggered_wards"] or 0)
    trigger_component = triggered_5d / total_wards

    # ── 3. Critical component: % wards critical in last 5 days ───────────
    crit_row = await conn.execute(text("""
        SELECT COUNT(DISTINCT ward_id) AS critical_wards
        FROM dispatch_runs
        WHERE status = 'critical'
        AND   created_at >= :cutoff
    """), {"cutoff": cutoff_5d})
    crit_wards = int((crit_row.scalar_one() or 0))
    critical_component = crit_wards / total_wards

    # ── 4. Capacity component: mean drain capacity degradation ───────────
    cap_row = await conn.execute(text("""
        SELECT AVG(capacity_c) AS mean_capacity
        FROM hotspots
    """))
    mean_cap = float(cap_row.scalar_one() or 100.0)
    # Invert: 100% capacity = 0 stress, 0% capacity = 1.0 stress
    capacity_component = max(0.0, (100.0 - mean_cap) / 100.0)

    # ── Composite SSI ─────────────────────────────────────────────────────
    ssi_raw = (
        0.40 * rain_component
        + 0.30 * trigger_component
        + 0.20 * critical_component
        + 0.10 * capacity_component
    ) * 100.0

    ssi = round(min(100.0, max(0.0, ssi_raw)), 1)
    level, colour = _ssi_level(ssi)

    # ── Recent dispatch stats for context ────────────────────────────────
    recent_row = await conn.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE status IN ('triggered','dispatched','critical')
                             AND created_at >= :cutoff_1d) AS triggered_24h,
            COUNT(*) FILTER (WHERE status = 'critical'
                             AND created_at >= :cutoff_1d) AS critical_24h
        FROM dispatch_runs
    """), {"cutoff_1d": cutoff_1d})
    recent = recent_row.mappings().first()

    result = {
        "ssi":                  ssi,
        "level":                level,
        "colour":               colour,
        "components": {
            "rain_pct":         round(rain_component     * 100, 1),
            "trigger_pct":      round(trigger_component  * 100, 1),
            "critical_pct":     round(critical_component * 100, 1),
            "capacity_stress_pct": round(capacity_component * 100, 1),
        },
        "context": {
            "rain_events_5d":   int(rain_data["event_count"] or 0),
            "triggered_5d":     triggered_5d,
            "critical_5d":      crit_wards,
            "triggered_24h":    int(recent["triggered_24h"] or 0),
            "critical_24h":     int(recent["critical_24h"]  or 0),
        },
        "thresholds": {
            "watch":        50,
            "pre_position": 75,
        },
        "computed_at": datetime.utcnow().isoformat(),
    }

    _cache["result"] = result
    _cache["ts"] = now
    return result


def invalidate_ssi_cache():
    """Call after any rain ingest or cycle run to force fresh SSI."""
    _cache["ts"] = 0.0
