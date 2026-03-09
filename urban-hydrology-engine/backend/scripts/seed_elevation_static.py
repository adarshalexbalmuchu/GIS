"""
seed_elevation_static.py — Populate ward_elevation from pre-generated JSON.

Uses data/delhi_ward_elevation.json (real SRTM elevations fetched via
Open-Meteo elevation API, matched to Delhi wards by centroid order).
No SRTM .tif files needed — works on any deployment including Render.
"""

import json
import os
import re

import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://hydro:hydro123@localhost:5432/hydrology")

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "delhi_ward_elevation.json")


def main():
    url = re.sub(r"^postgres(ql)?://", "postgresql://", DATABASE_URL)
    _is_local = any(h in url for h in ("localhost", "127.0.0.1", "@db:"))
    if not _is_local and "sslmode=" not in url:
        url += "?sslmode=require" if "?" not in url else "&sslmode=require"

    data_path = os.path.abspath(DATA_FILE)
    with open(data_path) as f:
        records = json.load(f)

    conn = psycopg2.connect(url)
    cur = conn.cursor()

    # Get ward IDs ordered by id (same order as geojson features)
    cur.execute("SELECT id FROM wards ORDER BY id")
    ward_ids = [row[0] for row in cur.fetchall()]

    if len(ward_ids) == 0:
        print("[seed_elevation_static] No wards found — skipping")
        cur.close()
        conn.close()
        return

    if len(ward_ids) != len(records):
        print(f"[seed_elevation_static] Ward count mismatch: DB={len(ward_ids)}, JSON={len(records)} — using min")

    inserted = 0
    for i, ward_id in enumerate(ward_ids):
        if i >= len(records):
            break
        r = records[i]
        elev = r["mean_elevation"]
        tc   = r["terrain_class"]
        rt   = r["runoff_t"]

        cur.execute("""
            INSERT INTO ward_elevation
                (ward_id, mean_elevation, min_elevation, max_elevation,
                 elevation_range, mean_slope, max_slope, runoff_t, terrain_class)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ward_id) DO UPDATE SET
                mean_elevation = EXCLUDED.mean_elevation,
                terrain_class  = EXCLUDED.terrain_class,
                runoff_t       = EXCLUDED.runoff_t
        """, (
            ward_id,
            elev,
            elev - 5.0,   # approximate min
            elev + 5.0,   # approximate max
            10.0,         # approximate range
            1.5,          # approximate mean slope
            3.0,          # approximate max slope
            rt,
            tc,
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"[seed_elevation_static] Inserted/updated {inserted} ward elevation records")


if __name__ == "__main__":
    main()
