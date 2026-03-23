"""
regenerate_hotspots_from_elevation.py — Replace randomly placed hotspots with
elevation-derived flood risk points.

For each ward, points are generated inside a buffer around the ward centroid
sized by mean_elevation:
  < 207m  → 800m buffer  (Yamuna floodplain — dense cluster)
  < 210m  → 500m buffer
  < 215m  → 300m buffer
  ≥ 215m  → 150m buffer  (high south Delhi — sparse, drain-proximate only)

capacity_c is derived from elevation:
  capacity_c = GREATEST(20, 100 - ((215 - mean_elevation) * 8))
  Lower elevation → lower drainage capacity.

critical_penalty_pc is preserved from existing OSM-proximity data.

Usage (from urban-hydrology-engine folder):
    docker-compose exec backend python scripts/regenerate_hotspots_from_elevation.py

    # or locally with DATABASE_URL set:
    python backend/scripts/regenerate_hotspots_from_elevation.py
"""

import os
import time

import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydro:hydro123@db:5432/hydrology"
)

# Points generated per ward by elevation tier
POINTS_BY_TIER = {
    207: 150,   # floodplain — many points
    210: 100,   # low flat
    215:  60,   # moderate relief
    999:  20,   # high / ridge — minimal
}


def n_points_for_elevation(mean_elev: float) -> int:
    for threshold, n in POINTS_BY_TIER.items():
        if mean_elev < threshold:
            return n
    return 20


def buffer_metres_for_elevation(mean_elev: float) -> int:
    if mean_elev < 207:
        return 800
    if mean_elev < 210:
        return 500
    if mean_elev < 215:
        return 300
    return 150


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ── Pre-flight checks ────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM ward_elevation")
    we_count = cur.fetchone()[0]
    if we_count == 0:
        print("ERROR: ward_elevation is empty. Run seed_elevation_static.py first.")
        conn.close()
        return

    cur.execute("SELECT COUNT(*) FROM wards")
    ward_count = cur.fetchone()[0]
    print(f"Wards: {ward_count}  |  ward_elevation rows: {we_count}")

    # ── Fetch current penalty map before wiping hotspots ────────
    # We preserve critical_penalty_pc since it comes from real OSM data.
    # Map: ward_id → list of penalty values (one per existing hotspot).
    # After regeneration we randomly assign these to new points so the
    # spatial penalty distribution is approximately preserved per ward.
    print("\nStep 1: Snapshot existing critical_penalty_pc values per ward...")
    cur.execute("""
        SELECT ward_id, ARRAY_AGG(critical_penalty_pc ORDER BY id) AS penalties
        FROM hotspots
        GROUP BY ward_id
    """)
    penalty_map: dict[int, list[float]] = {
        row[0]: list(row[1]) for row in cur.fetchall()
    }
    print(f"  Snapshotted penalties for {len(penalty_map)} wards.")

    # ── Delete all existing hotspots ────────────────────────────
    print("\nStep 2: Deleting all existing hotspots...")
    cur.execute("DELETE FROM hotspots")
    deleted = cur.rowcount
    conn.commit()
    print(f"  Deleted {deleted:,} hotspots.")

    # ── Generate new elevation-derived hotspot points ────────────
    print("\nStep 3: Generating elevation-derived points per ward...")
    t0 = time.time()

    cur.execute("""
        SELECT
            w.id        AS ward_id,
            we.mean_elevation,
            we.runoff_t
        FROM wards w
        JOIN ward_elevation we ON we.ward_id = w.id
        ORDER BY w.id
    """)
    wards = cur.fetchall()

    total_inserted = 0
    skipped_wards = 0

    for ward_id, mean_elev, runoff_t in wards:
        if mean_elev is None:
            skipped_wards += 1
            continue

        n_pts = n_points_for_elevation(mean_elev)
        buf_m  = buffer_metres_for_elevation(mean_elev)

        # capacity_c: lower elevation → lower drainage capacity
        capacity_c = max(20.0, 100.0 - ((215.0 - mean_elev) * 8.0))
        capacity_c = round(min(capacity_c, 100.0), 2)

        # Generate points inside the intersection of the ward geometry and a
        # centroid buffer.  ST_GeneratePoints is available in PostGIS ≥ 2.3.
        cur.execute("""
            SELECT (ST_Dump(
                ST_GeneratePoints(
                    ST_Intersection(
                        w.geom,
                        ST_Buffer(
                            ST_Centroid(w.geom)::geography,
                            %(buf_m)s
                        )::geometry
                    ),
                    %(n_pts)s
                )
            )).geom
            FROM wards w
            WHERE w.id = %(ward_id)s
        """, {"buf_m": buf_m, "n_pts": n_pts, "ward_id": ward_id})

        point_rows = cur.fetchall()
        if not point_rows:
            # Intersection was empty (very small ward vs large buffer is fine —
            # fall back to generating directly inside the ward geometry).
            cur.execute("""
                SELECT (ST_Dump(ST_GeneratePoints(w.geom, %(n_pts)s))).geom
                FROM wards w
                WHERE w.id = %(ward_id)s
            """, {"n_pts": n_pts, "ward_id": ward_id})
            point_rows = cur.fetchall()

        if not point_rows:
            skipped_wards += 1
            continue

        # Re-use snapshotted penalties for this ward, cycling if needed
        ward_penalties = penalty_map.get(ward_id, [0.0])

        insert_rows = []
        for i, (geom,) in enumerate(point_rows):
            penalty = ward_penalties[i % len(ward_penalties)]
            insert_rows.append((
                ward_id,
                geom,           # already a geometry object from PostGIS
                capacity_c,
                runoff_t if runoff_t is not None else 1.3,
                1.0,            # priority_weight (neutral default)
                float(penalty),
            ))

        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO hotspots
                (ward_id, geom, capacity_c, runoff_t, priority_weight, critical_penalty_pc)
            VALUES %s
            """,
            insert_rows,
            template="(%s, ST_SetSRID(%s::geometry, 4326), %s, %s, %s, %s)",
        )
        total_inserted += len(insert_rows)

    conn.commit()
    elapsed = time.time() - t0

    # ── Summary ──────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM hotspots")
    final_count = cur.fetchone()[0]

    print(f"\n  Points inserted : {total_inserted:,}")
    print(f"  Final DB count  : {final_count:,}")
    print(f"  Wards skipped   : {skipped_wards}")
    print(f"  Time elapsed    : {elapsed:.1f}s")

    # ── Elevation tier breakdown ─────────────────────────────────
    print("\nHotspot density by elevation tier:")
    cur.execute("""
        SELECT
            CASE
                WHEN we.mean_elevation < 207 THEN '< 207m  (floodplain)'
                WHEN we.mean_elevation < 210 THEN '207-210m (low flat)'
                WHEN we.mean_elevation < 215 THEN '210-215m (moderate)'
                ELSE                              '>= 215m  (high/ridge)'
            END AS tier,
            COUNT(DISTINCT h.ward_id) AS wards,
            COUNT(h.id)               AS hotspots
        FROM hotspots h
        JOIN ward_elevation we ON we.ward_id = h.ward_id
        GROUP BY tier
        ORDER BY MIN(we.mean_elevation)
    """)
    for tier, wards, hotspots in cur.fetchall():
        print(f"  {tier:<28} {wards:>4} wards   {hotspots:>7,} hotspots")

    print("\nDone. Yamuna corridor wards have dense clusters; high-elevation wards are sparse.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
