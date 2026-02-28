"""
Demo pusher — simulates a cloudburst over the real Delhi ward grid.

Dynamically fetches city bounding box and hotspot IDs from the API/DB
so it works with any ward dataset (mock grid or real Delhi boundaries).

Phases:
  1. Baseline sensor noise (pre-storm)
  2. Cloudburst rainfall polygon (central ~40%)
  3. Capacity degradation (storm impact)
  4. Summary
"""

import os
import random
import time

import httpx
import psycopg2

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://hydro:hydro123@localhost:5432/hydrology")
DELAY = 0.05  # seconds between requests
HEADERS = {
    "Content-Type": "application/json",
    "X-API-Key": os.getenv("API_SECRET_KEY", "hydro-mvp-secret-2026"),
}


def _post(client: httpx.Client, path: str, payload: dict) -> dict | None:
    """Fire a POST and return JSON, or print error and return None."""
    try:
        resp = client.post(f"{BASE_URL}{path}", json=payload, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"  ERROR {path}: {exc}")
        return None


def _get_city_bounds():
    """Query the DB for the bounding box of all wards, return central ~40%."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT ST_XMin(ext), ST_YMin(ext), ST_XMax(ext), ST_YMax(ext)
        FROM (SELECT ST_Extent(geom) AS ext FROM wards) sub
    """)
    xmin, ymin, xmax, ymax = cur.fetchone()
    cur.close()
    conn.close()

    # Shrink by 30% on each side to get central polygon
    dx = (xmax - xmin) * 0.30
    dy = (ymax - ymin) * 0.30
    return {
        "type": "Polygon",
        "coordinates": [[
            [xmin + dx, ymin + dy],
            [xmax - dx, ymin + dy],
            [xmax - dx, ymax - dy],
            [xmin + dx, ymax - dy],
            [xmin + dx, ymin + dy],
        ]],
    }


def _get_random_hotspot_ids(n: int = 100):
    """Fetch N random hotspot IDs from the database."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT id FROM hotspots ORDER BY RANDOM() LIMIT %s", (n,))
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return ids


def main():
    random.seed(0)
    rain_count = 0
    sensor_count = 0
    hotspots_in_rain = 0

    # Pre-fetch dynamic data from DB
    print("Fetching city bounds and hotspot IDs from DB...")
    rain_polygon = _get_city_bounds()
    hotspot_ids = _get_random_hotspot_ids(200)
    if not hotspot_ids:
        print("ERROR: No hotspots found in DB. Run import_delhi_wards.py first.")
        return
    print(f"  City bounds polygon ready, {len(hotspot_ids)} hotspot IDs loaded")

    with httpx.Client() as client:
        # ── Phase 1 — baseline sensor noise ──────────────────────────
        print("Phase 1: Sending baseline sensor noise...")
        noise_ids = random.sample(hotspot_ids, min(20, len(hotspot_ids)))
        for hid in noise_ids:
            delta = round(random.uniform(-5, 5), 2)
            resp = _post(client, "/ingest/sensor", {
                "hotspot_id": hid,
                "delta_capacity": delta,
            })
            if resp:
                sensor_count += 1
            time.sleep(DELAY)

        # ── Phase 2 — cloudburst rainfall polygon ────────────────────
        print("Phase 2: Sending cloudburst rain event...")
        resp = _post(client, "/ingest/rain", {
            "geojson_polygon": rain_polygon,
            "intensity_r": 8.5,
        })
        if resp:
            rain_count += 1
            hotspots_in_rain = resp.get("hotspots_in_polygon", 0)
            print(f"  Rain event stored — {hotspots_in_rain} hotspots in rain zone")
        time.sleep(DELAY)

        # ── Phase 3 — capacity degradation ───────────────────────────
        print("Phase 3: Simulating drainage capacity drop...")
        degrade_ids = random.sample(hotspot_ids, min(50, len(hotspot_ids)))
        for hid in degrade_ids:
            delta = round(random.uniform(-15, -5), 2)
            resp = _post(client, "/ingest/sensor", {
                "hotspot_id": hid,
                "delta_capacity": delta,
            })
            if resp:
                sensor_count += 1
            time.sleep(DELAY)

    # ── Phase 4 — summary ────────────────────────────────────────────
    print()
    print("=== CLOUDBURST SIMULATION COMPLETE ===")
    print(f"Rain events pushed:    {rain_count}")
    print(f"Sensor events pushed:  {sensor_count}")
    print(f"Hotspots in rain zone: {hotspots_in_rain}")
    print("Ready to run: POST /run/cycle to compute PMRS scores")


if __name__ == "__main__":
    main()
