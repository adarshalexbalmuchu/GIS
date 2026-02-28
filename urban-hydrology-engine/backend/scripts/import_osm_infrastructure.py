"""
import_osm_infrastructure.py — Load OSM critical infrastructure into PostGIS.

Reads 3 JSON files from backend/data/osm/ (hospitals, substations, fire stations)
and inserts them into the `critical_infrastructure` table.

Usage:
    docker-compose exec backend python scripts/import_osm_infrastructure.py
"""

import json
import os

import psycopg2

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydro:hydro123@db:5432/hydrology"
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "osm")

FILES = [
    ("hospitals.json",     "hospital"),
    ("substations.json",   "substation"),
    ("fire_stations.json", "fire_station"),
]


def extract_points(elements):
    """Extract (osm_id, lat, lon, name) from Overpass JSON elements."""
    points = []
    for e in elements:
        if e["type"] == "node":
            lat, lon = e["lat"], e["lon"]
        elif e["type"] == "way" and "center" in e:
            lat, lon = e["center"]["lat"], e["center"]["lon"]
        else:
            continue  # skip relations or ways without center
        name = e.get("tags", {}).get("name", None)
        points.append((e["id"], lat, lon, name))
    return points


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ── Create table ────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS critical_infrastructure (
            id SERIAL PRIMARY KEY,
            osm_id BIGINT,
            facility_type VARCHAR NOT NULL,
            name VARCHAR,
            lat FLOAT NOT NULL,
            lon FLOAT NOT NULL,
            geom GEOMETRY(POINT, 4326) NOT NULL
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_critical_infra_geom
        ON critical_infrastructure USING GIST(geom);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_critical_infra_type
        ON critical_infrastructure(facility_type);
    """)
    conn.commit()

    # ── Clear existing data (idempotent re-runs) ────────────────
    cur.execute("DELETE FROM critical_infrastructure")
    conn.commit()

    print("Importing OSM critical infrastructure...\n")
    total = 0

    for filename, facility_type in FILES:
        filepath = os.path.join(DATA_DIR, filename)
        with open(filepath) as f:
            data = json.load(f)

        points = extract_points(data["elements"])

        rows = [
            (osm_id, facility_type, name, lat, lon)
            for osm_id, lat, lon, name in points
        ]

        cur.executemany(
            """
            INSERT INTO critical_infrastructure
                (osm_id, facility_type, name, lat, lon, geom)
            VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            """,
            [
                (osm_id, ftype, name, lat, lon, lon, lat)
                for osm_id, ftype, name, lat, lon in rows
            ],
        )
        conn.commit()

        label = facility_type.replace("_", " ").capitalize() + "s"
        count = len(rows)
        total += count
        print(f"  {label:20s} {count:>4d} points imported")

    # ── Summary ─────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM critical_infrastructure")
    db_total = cur.fetchone()[0]

    print(f"\n  {'Total:':20s} {db_total:>4d} points in critical_infrastructure table")
    print("  Spatial index created.")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
