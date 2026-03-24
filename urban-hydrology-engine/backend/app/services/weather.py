"""Weather data — live Delhi rainfall from IMD and OpenWeatherMap."""

import os
import re
from datetime import datetime

import httpx

OWM_API_KEY    = os.getenv("OWM_API_KEY", "")
CITY_LAT       = float(os.getenv("CITY_LAT", "28.65"))
CITY_LON       = float(os.getenv("CITY_LON", "77.22"))
RAIN_COVERAGE  = float(os.getenv("RAIN_COVERAGE_DEGREES", "0.15"))

OWM_URL = "https://api.openweathermap.org/data/2.5/weather"
IMD_URL = "https://city.imd.gov.in/citywx/cityweather_imd.php?id=28749"

# Cache last weather status for the /weather/status endpoint
_last_status: dict = {
    "condition":            "unknown",
    "temp_c":               None,
    "intensity_r":          0.0,
    "is_raining":           False,
    "last_checked":         None,
    "source":               "OpenWeatherMap",
    "local_rainfall_mm_hr": None,
    "rainfall_source":      None,
}


# ── IMD scraper ────────────────────────────────────────────────────────────
async def _fetch_imd_rainfall(client: httpx.AsyncClient) -> float | None:
    """
    Fetch IMD Delhi city weather page and extract the last 24h or current
    rainfall figure in mm.  Returns mm/hr (rounded) or None on failure.
    """
    try:
        resp = await client.get(IMD_URL, timeout=12,
                                headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        html = resp.text

        # IMD pages use various label spellings — try them in priority order.
        # Pattern: "Rainfall" or "Rain Fall" label, then a number (possibly
        # separated by </td><td> or similar table markup).
        patterns = [
            # e.g. "24 hr Rainfall</td><td>12.4"
            r'(?i)(?:24\s*hr|today|current)\s*rain(?:fall)?[^<]{0,60}?(\d+\.?\d*)\s*(?:mm)?',
            # e.g. "Rain<br>12.4 mm"
            r'(?i)rain(?:fall)?.*?(\d+\.?\d*)\s*mm',
            # Generic fallback: any mm value near "rain"
            r'(?i)rain[^.]{0,120}?(\d+\.?\d*)\s*mm',
        ]
        for pat in patterns:
            m = re.search(pat, html, re.DOTALL)
            if m:
                mm = float(m.group(1))
                if mm > 0:
                    # IMD reports 24h accumulation; convert to mm/hr
                    return round(mm / 24, 2)
                # Zero is valid (no rain)
                return 0.0
    except Exception:
        pass
    return None


# ── OWM helper ────────────────────────────────────────────────────────────
async def _fetch_owm(client: httpx.AsyncClient) -> dict | None:
    """Return parsed OWM current-weather dict or None on error/no-key."""
    if not OWM_API_KEY:
        return None
    try:
        resp = await client.get(OWM_URL, params={
            "lat": CITY_LAT, "lon": CITY_LON,
            "appid": OWM_API_KEY, "units": "metric",
        }, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


# ── Main fetch ────────────────────────────────────────────────────────────
async def fetch_delhi_rainfall() -> list[dict]:
    """
    Fetch live Delhi rainfall.  Try IMD first, then OWM.
    Returns a list of rain-event dicts for ingest_rain_internal().
    """
    global _last_status

    async with httpx.AsyncClient() as client:

        # ── IMD ────────────────────────────────────────────────────────────
        imd_mm_hr = await _fetch_imd_rainfall(client)

        # ── OWM ────────────────────────────────────────────────────────────
        owm_data = await _fetch_owm(client)

    # ── Build unified status ───────────────────────────────────────────────
    # Prefer IMD for local mm/hr; OWM for condition, temp, area polygon.
    local_mm_hr    = None
    rainfall_src   = None
    intensity      = 0.0
    is_raining     = False
    condition_desc = "unknown"
    temp_c         = None
    rain_polygon   = None

    if imd_mm_hr is not None:
        local_mm_hr  = imd_mm_hr
        rainfall_src = "IMD"
        intensity    = imd_mm_hr
        is_raining   = imd_mm_hr > 0

    if owm_data:
        rain_1h = owm_data.get("rain", {}).get("1h", 0.0)
        rain_3h = owm_data.get("rain", {}).get("3h", 0.0)
        owm_intensity = rain_1h or (rain_3h / 3 if rain_3h else 0.0)
        condition_id  = owm_data["weather"][0]["id"]
        condition_desc = owm_data["weather"][0]["description"]
        is_raining    = is_raining or (condition_id < 700)
        temp_c        = owm_data.get("main", {}).get("temp")
        lat = owm_data["coord"]["lat"]
        lon = owm_data["coord"]["lon"]
        rain_polygon  = {
            "type": "Polygon",
            "coordinates": [[
                [lon - RAIN_COVERAGE, lat - RAIN_COVERAGE],
                [lon + RAIN_COVERAGE, lat - RAIN_COVERAGE],
                [lon + RAIN_COVERAGE, lat + RAIN_COVERAGE],
                [lon - RAIN_COVERAGE, lat + RAIN_COVERAGE],
                [lon - RAIN_COVERAGE, lat - RAIN_COVERAGE],
            ]],
        }
        # If IMD gave us no reading, fall back to OWM for local_mm_hr
        if local_mm_hr is None:
            local_mm_hr  = round(owm_intensity, 2)
            rainfall_src = "OWM"
            intensity    = owm_intensity
        else:
            # We have both — use the higher value and note both sources
            if owm_intensity > intensity:
                intensity = owm_intensity
            rainfall_src = "IMD/OWM"

    _last_status = {
        "condition":            condition_desc,
        "temp_c":               temp_c,
        "intensity_r":          round(intensity, 2),
        "is_raining":           is_raining,
        "last_checked":         datetime.utcnow().isoformat(),
        "source":               "IMD/OWM" if owm_data and imd_mm_hr is not None
                                else ("IMD" if imd_mm_hr is not None else "OpenWeatherMap"),
        "local_rainfall_mm_hr": local_mm_hr,
        "rainfall_source":      rainfall_src,
    }

    if not is_raining or rain_polygon is None:
        return []

    return [{
        "geojson_polygon": rain_polygon,
        "intensity_r":     max(intensity, 0.1),
        "source":          "imd_owm",
        "condition":       condition_desc,
        "raw_rain_1h":     intensity,
    }]


async def get_weather_status() -> dict:
    """Return the most recent weather status (cached from last poll)."""
    return dict(_last_status)
