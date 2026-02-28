"""
Import real Delhi ward boundaries from Datameet GeoJSON.

Replaces all mock data with 290 real MCD wards and generates
hotspots proportional to ward area using ST_GeneratePoints.
"""

import json
import os
import random

import psycopg2
import psycopg2.extras
from shapely.geometry import shape

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://hydro:hydro123@localhost:5432/hydrology",
)

GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "delhi_wards.geojson")

# Hotspot density: 1 per N sq metres of ward area
SQ_M_PER_HOTSPOT = 15_000
MIN_HOTSPOTS = 5
MAX_HOTSPOTS = 150

BATCH_CHUNK = 500


# ---------------------------------------------------------------------------
# Zone derivation (no zone_name in the GeoJSON — derive from Ward_No prefix)
# ---------------------------------------------------------------------------

def _derive_zone(ward_no: str | None, ward_name: str) -> str:
    """Best-effort zone from ward number prefix or name keywords."""
    if not ward_no:
        ward_no = ""
    wno_upper = ward_no.upper()

    if wno_upper.startswith("NDMC"):
        return "New Delhi"
    if wno_upper.startswith("CANT"):
        return "Cantonment"

    # Try extracting numeric part for MCD wards
    digits = "".join(c for c in ward_no if c.isdigit())
    if digits:
        n = int(digits)
        if n <= 58:
            return "South Delhi"
        if n <= 104:
            return "North Delhi"
        if n <= 150:
            return "East Delhi"
        if n <= 200:
            return "West Delhi"
        if n <= 250:
            return "Central Delhi"
        return "Outer Delhi"

    # Fallback based on keywords in ward name
    name_up = ward_name.upper()
    for zone_kw, zone_lbl in [
        ("SOUTH", "South Delhi"), ("NORTH", "North Delhi"),
        ("EAST", "East Delhi"), ("WEST", "West Delhi"),
        ("CENTRAL", "Central Delhi"), ("NEW DELHI", "New Delhi"),
    ]:
        if zone_kw in name_up:
            return zone_lbl

    return "Delhi"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    random.seed(42)

    # ── Load GeoJSON ─────────────────────────────────────────────
    with open(GEOJSON_PATH) as f:
        geojson = json.load(f)

    features = geojson["features"]
    print(f"Loaded {len(features)} features from GeoJSON")

    # ── Connect ──────────────────────────────────────────────────
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    conn.commit()

    # ── Clear existing data (FK order) ───────────────────────────
    print("Clearing existing data...")
    for tbl in ("dispatch_runs", "sensor_events", "rain_events", "hotspots", "wards"):
        cur.execute(f"DELETE FROM {tbl}")
    conn.commit()

    # ── Insert wards ─────────────────────────────────────────────
    print("Importing wards...")
    ward_rows = []
    skipped = 0

    for feat in features:
        props = feat.get("properties", {})
        ward_name = props.get("Ward_Name") or props.get("ward_name") or "Unknown"
        ward_no = props.get("Ward_No") or props.get("ward_no")
        zone = _derive_zone(ward_no, ward_name)

        try:
            geom = shape(feat["geometry"])
            wkt = geom.wkt
        except Exception as exc:
            print(f"  WARN: skipping {ward_name} — {exc}")
            skipped += 1
            continue

        ewkt = f"SRID=4326;{wkt}"
        ward_rows.append((ward_name, ward_no, zone, ewkt))

    # Batch insert wards, using ST_MakeValid
    insert_sql = """
        INSERT INTO wards (name, ward_no, zone_name, geom)
        VALUES %s
    """
    template = "(%(name)s, %(ward_no)s, %(zone)s, ST_MakeValid(ST_GeomFromEWKT(%(ewkt)s)))"
    dict_rows = [
        {"name": r[0], "ward_no": r[1], "zone": r[2], "ewkt": r[3]}
        for r in ward_rows
    ]
    psycopg2.extras.execute_values(
        cur, insert_sql, dict_rows,
        template="(%(name)s, %(ward_no)s, %(zone)s, ST_MakeValid(ST_GeomFromEWKT(%(ewkt)s)))",
        page_size=500,
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM wards")
    ward_count = cur.fetchone()[0]
    print(f"Imported {ward_count} wards ({skipped} skipped)")

    # ── Generate hotspots per ward using ST_GeneratePoints ───────
    print("Generating hotspots...")

    # Get ward id, zone, and area in sq metres
    cur.execute("""
        SELECT id, zone_name, ST_Area(geom::geography) AS area_m2
        FROM wards
        ORDER BY id
    """)
    ward_info = cur.fetchall()

    total_hotspots = 0
    hotspot_batch = []

    for ward_id, zone, area_m2 in ward_info:
        n = int(area_m2 / SQ_M_PER_HOTSPOT)
        n = max(MIN_HOTSPOTS, min(MAX_HOTSPOTS, n))

        # Use ST_GeneratePoints to create random points inside the ward
        cur.execute("""
            SELECT ST_X(pt) AS lon, ST_Y(pt) AS lat
            FROM (
                SELECT (ST_Dump(ST_GeneratePoints(
                    ST_MakeValid(geom), %s
                ))).geom AS pt
                FROM wards WHERE id = %s
            ) sub
        """, (n, ward_id))
        points = cur.fetchall()

        for lon, lat in points:
            # Small bounding box around each point (0.001° ≈ 111m)
            half = 0.0005
            wkt = (
                f"SRID=4326;POLYGON(("
                f"{lon-half} {lat-half},"
                f"{lon+half} {lat-half},"
                f"{lon+half} {lat+half},"
                f"{lon-half} {lat+half},"
                f"{lon-half} {lat-half}))"
            )

            cap = round(random.uniform(40, 100), 2)
            roff = round(random.uniform(1.0, 3.5), 2)
            pw = round(random.uniform(0.5, 2.0), 2)
            crit = round(random.uniform(50, 200), 2) if random.random() < 0.08 else 0.0

            hotspot_batch.append((ward_id, wkt, cap, roff, pw, crit, zone))

        # Flush batch periodically
        if len(hotspot_batch) >= BATCH_CHUNK:
            _insert_hotspots(cur, hotspot_batch)
            total_hotspots += len(hotspot_batch)
            hotspot_batch = []

    # Flush remaining
    if hotspot_batch:
        _insert_hotspots(cur, hotspot_batch)
        total_hotspots += len(hotspot_batch)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM hotspots")
    hs_count = cur.fetchone()[0]
    print(f"Inserted {hs_count} hotspots across {ward_count} wards")

    # ── Verification ─────────────────────────────────────────────
    print("\nZone breakdown:")
    cur.execute("""
        SELECT w.zone_name,
               COUNT(DISTINCT w.id) AS wards,
               COUNT(h.id) AS hotspots
        FROM wards w
        LEFT JOIN hotspots h ON h.ward_id = w.id
        GROUP BY w.zone_name
        ORDER BY hotspots DESC
    """)
    for zone, wcount, hcount in cur.fetchall():
        print(f"  {zone:20s}  {wcount:>4d} wards  {hcount:>5d} hotspots")

    print(f"\nDone. Wards: {ward_count}, Hotspots: {hs_count}")

    cur.close()
    conn.close()


def _insert_hotspots(cur, rows):
    """Batch insert hotspot rows."""
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO hotspots
           (ward_id, geom, capacity_c, runoff_t, priority_weight, critical_penalty_pc, zone_name)
           VALUES %s""",
        rows,
        page_size=500,
    )


if __name__ == "__main__":
    main()
