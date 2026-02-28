"""
Seed script — generates 30 wards and ~2,550 hotspots for a mock Indian city.

Uses raw psycopg2 + batch inserts for speed (NOT SQLAlchemy ORM).
All geometry is WKT with SRID=4326.
"""

import os
import random
import asyncio

import psycopg2
import psycopg2.extras
from shapely.geometry import box


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://hydro:hydro123@localhost:5432/hydrology",
)

# Grid origin — New Delhi area
BASE_LAT = 28.60
BASE_LON = 77.20

WARD_DX = 0.02  # degrees longitude per ward
WARD_DY = 0.02  # degrees latitude per ward
GRID_COLS = 5
GRID_ROWS = 6

HOTSPOTS_PER_WARD = 85
HOTSPOT_SIZE = 0.002  # degrees

BATCH_CHUNK = 500  # rows per INSERT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ward_polygon_wkt(row: int, col: int) -> str:
    """Return the SRID=4326 WKT for one ward cell."""
    x0 = BASE_LON + col * WARD_DX
    y0 = BASE_LAT + row * WARD_DY
    poly = box(x0, y0, x0 + WARD_DX, y0 + WARD_DY)
    return f"SRID=4326;{poly.wkt}"


def _random_hotspot_wkt(row: int, col: int) -> str:
    """Return the SRID=4326 WKT for a small random hotspot inside a ward."""
    x0 = BASE_LON + col * WARD_DX
    y0 = BASE_LAT + row * WARD_DY
    # random bottom-left corner inside the ward, leaving room for the box
    hx = x0 + random.uniform(0, WARD_DX - HOTSPOT_SIZE)
    hy = y0 + random.uniform(0, WARD_DY - HOTSPOT_SIZE)
    poly = box(hx, hy, hx + HOTSPOT_SIZE, hy + HOTSPOT_SIZE)
    return f"SRID=4326;{poly.wkt}"


def _random_hotspot_attrs() -> tuple:
    """Return (capacity_c, runoff_t, priority_weight, critical_penalty_pc)."""
    capacity_c = round(random.uniform(40, 100), 2)
    runoff_t = round(random.uniform(1.0, 3.5), 2)
    priority_weight = round(random.uniform(0.5, 2.0), 2)
    # 10 % chance of being a critical facility (hospital / power grid)
    if random.random() < 0.10:
        critical_penalty_pc = round(random.uniform(50, 200), 2)
    else:
        critical_penalty_pc = 0.0
    return capacity_c, runoff_t, priority_weight, critical_penalty_pc


# ---------------------------------------------------------------------------
# Batch insert helpers
# ---------------------------------------------------------------------------

def _batch_insert(cur, sql: str, rows: list[tuple], chunk: int = BATCH_CHUNK):
    """Execute batch inserts in chunks."""
    for i in range(0, len(rows), chunk):
        psycopg2.extras.execute_values(cur, sql, rows[i : i + chunk])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    random.seed(42)

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # Ensure PostGIS
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    conn.commit()

    # ------ Wards ------
    print("Inserting wards...")
    ward_rows: list[tuple] = []
    ward_index = 1
    ward_grid: list[tuple[int, int, int]] = []  # (ward_id, row, col)

    for r in range(GRID_ROWS):
        for c in range(GRID_COLS):
            name = f"Ward_{ward_index:02d}"
            wkt = _ward_polygon_wkt(r, c)
            ward_rows.append((name, wkt))
            ward_grid.append((ward_index, r, c))
            ward_index += 1

    _batch_insert(
        cur,
        "INSERT INTO wards (name, geom) VALUES %s",
        [(name, wkt) for name, wkt in ward_rows],
    )
    conn.commit()

    # ------ Hotspots ------
    total_hotspots = HOTSPOTS_PER_WARD * len(ward_grid)
    print(f"Inserting {total_hotspots} hotspots...")
    hotspot_rows: list[tuple] = []

    for ward_id, r, c in ward_grid:
        for _ in range(HOTSPOTS_PER_WARD):
            wkt = _random_hotspot_wkt(r, c)
            cap, roff, pw, crit = _random_hotspot_attrs()
            hotspot_rows.append((ward_id, wkt, cap, roff, pw, crit))

    _batch_insert(
        cur,
        "INSERT INTO hotspots (ward_id, geom, capacity_c, runoff_t, priority_weight, critical_penalty_pc) VALUES %s",
        hotspot_rows,
    )
    conn.commit()

    # ------ Summary ------
    cur.execute("SELECT COUNT(*) FROM wards")
    ward_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM hotspots")
    hotspot_count = cur.fetchone()[0]

    print(f"Done. Wards: {ward_count}, Hotspots: {hotspot_count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
