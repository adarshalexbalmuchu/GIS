"""OpenWeatherMap integration — fetch live Delhi rainfall data."""

import os
from datetime import datetime

import httpx

OWM_API_KEY = os.getenv("OWM_API_KEY", "")
CITY_LAT = float(os.getenv("CITY_LAT", "28.65"))
CITY_LON = float(os.getenv("CITY_LON", "77.22"))
RAIN_COVERAGE = float(os.getenv("RAIN_COVERAGE_DEGREES", "0.15"))

OWM_URL = "https://api.openweathermap.org/data/2.5/weather"

# Cache last weather status for the /weather/status endpoint
_last_status: dict = {
    "condition": "unknown",
    "temp_c": None,
    "intensity_r": 0.0,
    "is_raining": False,
    "last_checked": None,
    "source": "OpenWeatherMap",
}


async def fetch_delhi_rainfall() -> list[dict]:
    """
    Call OWM current-weather API and return a list of rain event dicts
    suitable for ingest_rain_internal(). Returns [] if not raining or on error.
    """
    global _last_status

    if not OWM_API_KEY:
        print("[weather] OWM_API_KEY not set — skipping fetch")
        return []

    params = {
        "lat": CITY_LAT,
        "lon": CITY_LON,
        "appid": OWM_API_KEY,
        "units": "metric",
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(OWM_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        print(f"[weather] OWM request failed: {exc}")
        return []

    # Parse rainfall
    rain_1h = data.get("rain", {}).get("1h", 0.0)
    rain_3h = data.get("rain", {}).get("3h", 0.0)
    intensity = rain_1h or (rain_3h / 3 if rain_3h else 0.0)

    condition_id = data["weather"][0]["id"]
    condition_desc = data["weather"][0]["description"]
    is_raining = condition_id < 700  # 2xx=thunderstorm, 3xx=drizzle, 5xx=rain

    lat = data["coord"]["lat"]
    lon = data["coord"]["lon"]
    temp_c = data.get("main", {}).get("temp")

    # Update cached status
    _last_status = {
        "condition": condition_desc,
        "temp_c": temp_c,
        "intensity_r": round(intensity, 2),
        "is_raining": is_raining,
        "last_checked": datetime.utcnow().isoformat(),
        "source": "OpenWeatherMap",
    }

    if not is_raining:
        return []

    # Build rain event covering an area around the reported coords
    return [{
        "geojson_polygon": {
            "type": "Polygon",
            "coordinates": [[
                [lon - RAIN_COVERAGE, lat - RAIN_COVERAGE],
                [lon + RAIN_COVERAGE, lat - RAIN_COVERAGE],
                [lon + RAIN_COVERAGE, lat + RAIN_COVERAGE],
                [lon - RAIN_COVERAGE, lat + RAIN_COVERAGE],
                [lon - RAIN_COVERAGE, lat - RAIN_COVERAGE],
            ]],
        },
        "intensity_r": max(intensity, 0.1),
        "source": "openweathermap",
        "condition": condition_desc,
        "raw_rain_1h": rain_1h,
    }]


async def get_weather_status() -> dict:
    """Return the most recent weather status (cached from last poll)."""
    return dict(_last_status)
