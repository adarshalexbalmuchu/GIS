"""
update_hotspot_elevation.py — Update hotspot runoff_t from real elevation data.

Step 1: Inherit ward-level runoff_t for all hotspots.
Step 2: Low-point adjustment — hotspots in wards with meaningful relief
        get +0.3 (randomly ~20%, as a proxy for low-lying areas since
        hotspot geometries lack Z values from ST_GeneratePoints).

Usage:
    docker-compose exec backend python scripts/update_hotspot_elevation.py
"""

import os
import time

import psycopg2

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydro:hydro123@db:5432/hydrology"
)


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ── Verify ward_elevation exists and has data ───────────────
    cur.execute("SELECT COUNT(*) FROM ward_elevation")
    we_count = cur.fetchone()[0]
    if we_count == 0:
        print("ERROR: ward_elevation is empty. Run calculate_elevation.py first.")
        return

    print(f"Updating hotspot runoff_t from real elevation data...")
    print(f"  ward_elevation rows: {we_count}\n")

    t0 = time.time()

    # ── Step 1: Inherit ward runoff_t ───────────────────────────
    print("  Step 1: Ward inheritance...", end="", flush=True)
    cur.execute("""
        UPDATE hotspots h
        SET runoff_t = we.runoff_t
        FROM ward_elevation we
        WHERE we.ward_id = h.ward_id
    """)
    step1_count = cur.rowcount
    conn.commit()
    print(f" {step1_count:,} hotspots updated")

    # ── Step 2: Low-point adjustment (+0.3) ─────────────────────
    # For wards with elevation_range > 5m (meaningful relief),
    # randomly select ~20% of hotspots as low-point proxies
    # and add +0.3 to their runoff_t (capped at 3.5).
    print("  Step 2: Low-point adjustment (+0.3)...", end="", flush=True)
    cur.execute("""
        UPDATE hotspots h
        SET runoff_t = LEAST(3.5, h.runoff_t + 0.3)
        FROM ward_elevation we
        WHERE we.ward_id = h.ward_id
          AND we.elevation_range > 5
          AND RANDOM() < 0.20
    """)
    step2_count = cur.rowcount
    conn.commit()
    elapsed = time.time() - t0
    print(f" {step2_count:,} hotspots adjusted")

    # ── Summary ─────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM hotspots")
    total = cur.fetchone()[0]

    print(f"\n  Total hotspots: {total:,}")
    print(f"  Time elapsed: {elapsed:.1f}s\n")

    print("Final runoff_t distribution:")
    cur.execute("""
        SELECT ROUND(runoff_t::numeric, 1) AS rt, COUNT(*) AS cnt
        FROM hotspots
        GROUP BY ROUND(runoff_t::numeric, 1)
        ORDER BY rt
    """)
    for row in cur.fetchall():
        rt, cnt = float(row[0]), row[1]
        label = ""
        if rt == 1.0:
            label = "(floodplain)"
        elif rt == 1.3:
            label = "(flat)"
        elif rt == 1.6:
            label = "(low-point adjusted flat)"
        elif rt == 1.7:
            label = "(moderate)"
        elif rt == 2.0:
            label = "(low-point adjusted moderate)"
        elif rt == 2.2:
            label = "(steep)"
        elif rt == 2.5:
            label = "(low-point adjusted steep)"
        elif rt == 2.7:
            label = "(ridge)"
        elif rt == 3.0:
            label = "(low-point adjusted ridge)"
        elif rt == 3.5:
            label = "(ridge max)"
        print(f"  runoff_t={rt:.1f}: {cnt:>6,}  {label}")

    print(f"\nAll {total:,} hotspots have real elevation-based runoff multipliers.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
