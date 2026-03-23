"""
Yamuna / Hathnikund Advance Warning Service.

Fetches real-time discharge from CWC India-WRIS or falls back to
Open-Meteo upstream precipitation as a proxy when CWC is unavailable.

Alert thresholds (Old Railway Bridge, Delhi):
  Normal:  < 1,00,000 cusecs  — green
  Watch:     1,00,000 cusecs  — amber  (CWC issues forecast)
  Warning:   2,00,000 cusecs  — orange
  Danger:    3,00,000+ cusecs — red    (Yamuna likely > 205.33m in 48h)

Travel time: Hathnikund → Delhi ≈ 48–72 hours depending on discharge
"""

from datetime import datetime, timezone, timedelta
import httpx

_IST = timezone(timedelta(hours=5, minutes=30))
_CACHE_TTL = timedelta(minutes=15)
_cache: dict = {"data": None, "ts": None}

# Yamuna stage thresholds at Old Railway Bridge (metres)
YAMUNA_THRESHOLDS = {
    "normal":  202.0,
    "warning": 204.22,
    "danger":  205.33,
    "high_flood": 206.0,
    "extreme": 208.0,   # 2023 peak was 208.66m
}

# Discharge thresholds at Hathnikund (cusecs)
HATHNIKUND_THRESHOLDS = {
    "watch":   100_000,
    "warning": 200_000,
    "danger":  300_000,
    "extreme": 500_000,
}


def _discharge_to_alert(cusecs: float) -> dict:
    """Map Hathnikund discharge → alert level, predicted Delhi stage, ETA."""
    if cusecs >= HATHNIKUND_THRESHOLDS["extreme"]:
        level, colour = "EXTREME", "#7F1D1D"
        pred_stage = 208.0
        eta_hours  = 48
    elif cusecs >= HATHNIKUND_THRESHOLDS["danger"]:
        level, colour = "DANGER", "#DC2626"
        pred_stage = 207.0
        eta_hours  = 52
    elif cusecs >= HATHNIKUND_THRESHOLDS["warning"]:
        level, colour = "WARNING", "#D97706"
        pred_stage = 206.0
        eta_hours  = 56
    elif cusecs >= HATHNIKUND_THRESHOLDS["watch"]:
        level, colour = "WATCH", "#CA8A04"
        pred_stage = 205.0
        eta_hours  = 60
    else:
        level, colour = "NORMAL", "#16A34A"
        pred_stage = 202.0 + (cusecs / HATHNIKUND_THRESHOLDS["watch"]) * 2.0
        eta_hours  = 72

    eta_arrival = datetime.now(_IST) + timedelta(hours=eta_hours)

    return {
        "level":          level,
        "colour":         colour,
        "predicted_delhi_stage_m": round(pred_stage, 2),
        "travel_time_hours":       eta_hours,
        "eta_delhi":               eta_arrival.strftime("%b %d %H:%M IST"),
    }


async def fetch_yamuna_status() -> dict:
    """
    Fetch Yamuna / Hathnikund status.

    Primary:  CWC India-WRIS real-time gauge (public JSON endpoint).
    Fallback: Open-Meteo upstream precipitation as proxy discharge.
    """
    now = datetime.now(_IST)

    if (_cache["data"] and _cache["ts"]
            and (now - _cache["ts"]) < _CACHE_TTL):
        return _cache["data"]

    result = await _try_cwc_wris()
    if result is None:
        result = await _fallback_open_meteo()

    _cache["data"] = result
    _cache["ts"]   = now
    return result


async def _try_cwc_wris() -> dict | None:
    """
    Attempt to fetch from CWC India-WRIS open data.
    Returns None if unavailable (network, rate-limit, format change).
    """
    # CWC WRIS provides gauge data via their map service
    # Station: Hathnikund Barrage — CWC station ID 240400
    # Endpoint is a public WFS-style JSON — may change, so we handle gracefully
    try:
        url = (
            "https://indiawris.gov.in/wris/#/RealTime"
        )
        # Direct API for gauge readings (discovered from network inspection)
        api_url = (
            "https://indiawris.gov.in/wris/data/PublicReport/realTimeData"
            "?stationId=240400&dataType=DISCHARGE&lang=en"
        )
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(api_url)
            if resp.status_code != 200:
                return None
            data = resp.json()

        # Try to extract latest discharge value
        records = data.get("records", data.get("data", []))
        if not records:
            return None

        latest = records[0] if isinstance(records, list) else None
        if not latest:
            return None

        discharge_cusecs = float(latest.get("discharge", latest.get("value", 0)))
        obs_time = latest.get("obsTime", latest.get("date", ""))

        alert = _discharge_to_alert(discharge_cusecs)

        return {
            "source":              "CWC India-WRIS",
            "station":             "Hathnikund Barrage",
            "discharge_cusecs":    round(discharge_cusecs),
            "discharge_cumecs":    round(discharge_cusecs * 0.0283168, 1),
            "obs_time":            obs_time,
            "alert_level":         alert["level"],
            "alert_colour":        alert["colour"],
            "predicted_delhi_stage_m": alert["predicted_delhi_stage_m"],
            "travel_time_hours":   alert["travel_time_hours"],
            "eta_delhi":           alert["eta_delhi"],
            "yamuna_thresholds":   YAMUNA_THRESHOLDS,
            "is_live":             True,
        }
    except Exception:
        return None


async def _fallback_open_meteo() -> dict:
    """
    Proxy: fetch upstream (Himachal/Uttarakhand) precipitation from
    Open-Meteo and estimate Hathnikund discharge from it.
    Uses Haridwar coordinates (upstream catchment representative point).
    """
    try:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=29.95&longitude=78.16"   # Haridwar / upper Yamuna
            "&hourly=precipitation&timezone=Asia%2FKolkata"
            "&forecast_days=3&past_days=2"
        )
        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            payload = resp.json()

        times = payload["hourly"]["time"]
        rain  = payload["hourly"]["precipitation"]

        now_str = datetime.now(_IST).strftime("%Y-%m-%dT%H:00")
        try:
            idx = times.index(now_str)
        except ValueError:
            idx = len(times) // 2

        # 24h upstream cumulative rain
        start = max(0, idx - 24)
        cum_24h = sum(r for r in rain[start:idx] if r is not None)

        # Empirical proxy: 1mm upstream ≈ 5,000 cusecs at Hathnikund
        # (very rough but reasonable for demonstration)
        discharge_cusecs = min(600_000, cum_24h * 5_000)
        alert = _discharge_to_alert(discharge_cusecs)

        return {
            "source":              "Open-Meteo upstream proxy",
            "station":             "Hathnikund Barrage (estimated)",
            "discharge_cusecs":    round(discharge_cusecs),
            "discharge_cumecs":    round(discharge_cusecs * 0.0283168, 1),
            "obs_time":            now_str,
            "alert_level":         alert["level"],
            "alert_colour":        alert["colour"],
            "predicted_delhi_stage_m": alert["predicted_delhi_stage_m"],
            "travel_time_hours":   alert["travel_time_hours"],
            "eta_delhi":           alert["eta_delhi"],
            "upstream_24h_rain_mm": round(cum_24h, 1),
            "yamuna_thresholds":   YAMUNA_THRESHOLDS,
            "is_live":             False,
            "note": "CWC India-WRIS unavailable — discharge estimated from upstream Haridwar precipitation (Open-Meteo) using empirical proxy: 1mm upstream ≈ 5,000 cusecs at Hathnikund. Thresholds from CWC published alert levels.",
        }

    except Exception as exc:
        # Final fallback: return static safe state
        alert = _discharge_to_alert(0)
        return {
            "source":           "Unavailable",
            "station":          "Hathnikund Barrage",
            "discharge_cusecs": 0,
            "discharge_cumecs": 0,
            "obs_time":         datetime.now(_IST).isoformat(),
            "alert_level":      "UNKNOWN",
            "alert_colour":     "#94A3B8",
            "predicted_delhi_stage_m": 202.0,
            "travel_time_hours": 72,
            "eta_delhi":        "—",
            "yamuna_thresholds": YAMUNA_THRESHOLDS,
            "is_live":          False,
            "error":            str(exc),
        }
