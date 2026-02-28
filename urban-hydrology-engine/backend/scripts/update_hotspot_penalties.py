"""
update_hotspot_penalties.py — Recalculate hotspot critical_penalty_pc
from real OSM infrastructure proximity.

Runs 7 tiers HIGHEST FIRST using ST_DWithin with ::geography cast
for accurate metre-based distances. The WHERE critical_penalty_pc = 0
guard prevents lower tiers from overwriting higher ones.

Usage:
    docker-compose exec backend python scripts/update_hotspot_penalties.py
"""

import os
import time

import psycopg2

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydro:hydro123@db:5432/hydrology"
)

# (tier_name, facility_type_or_'any', distance_m, penalty_value)
TIERS = [
    ("hospital 100m",    "hospital",     100, 200),
    ("substation 100m",  "substation",   100, 150),
    ("fire stn 100m",    "fire_station", 100, 100),
    ("hospital 250m",    "hospital",     250, 100),
    ("substation 250m",  "substation",   250,  75),
    ("fire stn 250m",    "fire_station", 250,  50),
    ("any 500m",         None,           500,  25),
]


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ── Reset all penalties to 0 first ──────────────────────────
    print("Updating hotspot penalties from real OSM data...\n")
    print("  Resetting all critical_penalty_pc to 0...")
    cur.execute("UPDATE hotspots SET critical_penalty_pc = 0")
    conn.commit()

    total_start = time.time()

    for tier_name, facility_type, distance_m, penalty in TIERS:
        t0 = time.time()
        print(f"  Tier ({tier_name}): computing ST_DWithin {distance_m}m ...", end="", flush=True)

        if facility_type is not None:
            cur.execute(f"""
                UPDATE hotspots SET critical_penalty_pc = {penalty}
                WHERE critical_penalty_pc = 0
                AND id IN (
                    SELECT DISTINCT h.id FROM hotspots h
                    JOIN critical_infrastructure ci ON ci.facility_type = %s
                    WHERE ST_DWithin(h.geom::geography, ci.geom::geography, %s)
                )
            """, (facility_type, distance_m))
        else:
            # "any" facility type
            cur.execute(f"""
                UPDATE hotspots SET critical_penalty_pc = {penalty}
                WHERE critical_penalty_pc = 0
                AND id IN (
                    SELECT DISTINCT h.id FROM hotspots h
                    JOIN critical_infrastructure ci
                    ON ST_DWithin(h.geom::geography, ci.geom::geography, %s)
                )
            """, (distance_m,))

        updated = cur.rowcount
        conn.commit()
        elapsed = time.time() - t0
        print(f" updated {updated:>5d} hotspots → penalty {penalty:>3d}  ({elapsed:.1f}s)")

    # ── Summary ─────────────────────────────────────────────────
    total_elapsed = time.time() - total_start

    cur.execute("SELECT COUNT(*) FROM hotspots WHERE critical_penalty_pc = 0")
    non_critical = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM hotspots")
    total = cur.fetchone()[0]

    print(f"\n  Non-critical (penalty=0): {non_critical:>6d} hotspots")
    print(f"  Total hotspots:           {total:>6d}")
    print(f"  Time elapsed:             {total_elapsed:.1f}s\n")

    # ── Penalty distribution ────────────────────────────────────
    print("Penalty distribution:")
    cur.execute("""
        SELECT critical_penalty_pc, COUNT(*) AS cnt
        FROM hotspots
        GROUP BY critical_penalty_pc
        ORDER BY critical_penalty_pc DESC
    """)
    for row in cur.fetchall():
        penalty_val, cnt = row
        label = ""
        if penalty_val == 200: label = "(real hospital proximity)"
        elif penalty_val == 150: label = "(real substation proximity)"
        elif penalty_val == 0: label = "(genuinely non-critical)"
        print(f"  penalty={int(penalty_val):>3d}: {cnt:>6d}  {label}")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
