"""
6-Hour Predictive Risk — Open-Meteo forecast for Delhi.
Caches results for 10 minutes.
"""

from datetime import datetime, timezone, timedelta

import httpx

# Module-level cache
_cache: dict = {"data": None, "fetched_at": None}
_CACHE_TTL = timedelta(minutes=10)

# Delhi coords
_LAT = 28.65
_LON = 77.22
_IST = timezone(timedelta(hours=5, minutes=30))

# Risk thresholds (total 6h mm)
_RISK_LEVELS = [
    (50, "EXTREME", "#7F1D1D"),
    (25, "HIGH",    "#DC2626"),
    (10, "MODERATE","#D97706"),
    (2,  "LOW",     "#16A34A"),
    (0,  "NONE",    "#94A3B8"),
]


def _rainfall_to_triggered(total_mm: float) -> int:
    """Rough estimate of wards that would trigger given total 6h rain."""
    if total_mm < 2:
        return 0
    if total_mm < 10:
        return int(total_mm * 4)        # ~4 wards per mm
    if total_mm < 25:
        return int(40 + (total_mm - 10) * 5)   # ramp up
    if total_mm < 50:
        return int(115 + (total_mm - 25) * 4)
    return min(290, int(215 + (total_mm - 50) * 2))


async def fetch_delhi_forecast() -> dict:
    """Fetch next 6 hours of rainfall from Open-Meteo."""
    now = datetime.now(_IST)

    # Check cache
    if (_cache["data"] is not None
            and _cache["fetched_at"] is not None
            and (now - _cache["fetched_at"]) < _CACHE_TTL):
        return _cache["data"]

    # Fetch from Open-Meteo (free, no key required)
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={_LAT}&longitude={_LON}"
        "&hourly=rain&timezone=Asia%2FKolkata"
        "&forecast_days=2"
    )

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        payload = resp.json()

    hourly_times = payload.get("hourly", {}).get("time", [])
    hourly_rain = payload.get("hourly", {}).get("rain", [])

    # Find current hour index
    current_hour_str = now.strftime("%Y-%m-%dT%H:00")
    try:
        start_idx = hourly_times.index(current_hour_str)
    except ValueError:
        # Fallback: find closest hour
        start_idx = 0
        for i, t in enumerate(hourly_times):
            if t <= current_hour_str:
                start_idx = i

    # Extract next 6 hours
    hours = []
    peak_rain = 0.0
    peak_hour = ""
    total_6h = 0.0

    for i in range(start_idx, min(start_idx + 6, len(hourly_times))):
        t_str = hourly_times[i]
        rain = hourly_rain[i] if i < len(hourly_rain) else 0.0
        rain = rain if rain is not None else 0.0
        hour_label = t_str.split("T")[1][:5] if "T" in t_str else t_str

        hours.append({
            "time": hour_label,
            "rain_mm": round(rain, 1),
        })
        total_6h += rain
        if rain > peak_rain:
            peak_rain = rain
            peak_hour = hour_label

    total_6h = round(total_6h, 1)
    peak_rain = round(peak_rain, 1)

    # Risk level
    risk_level = "NONE"
    risk_colour = "#94A3B8"
    for threshold, level, colour in _RISK_LEVELS:
        if total_6h >= threshold:
            risk_level = level
            risk_colour = colour
            break

    predicted = _rainfall_to_triggered(total_6h)

    result = {
        "hours": hours,
        "peak_hour": peak_hour,
        "peak_rain_mm": peak_rain,
        "total_6h_mm": total_6h,
        "risk_level": risk_level,
        "risk_colour": risk_colour,
        "predicted_triggered": predicted,
    }

    # Update cache
    _cache["data"] = result
    _cache["fetched_at"] = now

    return result
