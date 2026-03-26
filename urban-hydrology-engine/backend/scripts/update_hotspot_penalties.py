"""
update_hotspot_penalties.py — Recalculate hotspot critical_penalty_pc and
priority_weight from real OSM infrastructure proximity.

critical_penalty_pc: 7 tiers HIGHEST FIRST using ST_DWithin with ::geography cast
for accurate metre-based distances. The WHERE critical_penalty_pc = 0
guard prevents lower tiers from overwriting higher ones.

priority_weight: reflects emergency service criticality.
  hospital 100m  → 2.0  (highest — flood near hospital is life-critical)
  substation 100m → 1.8  (power loss amplifies all other risks)
  fire_station 100m → 1.5  (response capacity at risk)
  hospital 250m  → 1.6
  substation 250m → 1.4
  fire_station 250m → 1.3
  any infra 500m → 1.2
  no proximity   → 1.0  (default)

Usage:
    docker-compose exec backend python scripts/update_hotspot_penalties.py
"""

import os
import time

import psycopg2

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydro:hydro123@db:5432/hydrology"
)

# (tier_name, facility_type_or_None, distance_m, penalty_value, priority_weight)
TIERS = [
    ("hospital 100m",    "hospital",     100, 200, 2.0),
    ("substation 100m",  "substation",   100, 150, 1.8),
    ("fire stn 100m",    "fire_station", 100, 100, 1.5),
    ("hospital 250m",    "hospital",     250, 100, 1.6),
    ("substation 250m",  "substation",   250,  75, 1.4),
    ("fire stn 250m",    "fire_station", 250,  50, 1.3),
    ("any 500m",         None,           500,  25, 1.2),
]


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ── Reset to defaults first ──────────────────────────────────
    print("Updating hotspot penalties and priority weights from real OSM data...\n")
    print("  Resetting all critical_penalty_pc to 0, priority_weight to 1.0...")
    cur.execute("UPDATE hotspots SET critical_penalty_pc = 0, priority_weight = 1.0")
    conn.commit()

    total_start = time.time()

    for tier_name, facility_type, distance_m, penalty, pw in TIERS:
        t0 = time.time()
        print(f"  Tier ({tier_name}): computing ST_DWithin {distance_m}m ...", end="", flush=True)

        if facility_type is not None:
            cur.execute(f"""
                UPDATE hotspots
                SET critical_penalty_pc = {penalty},
                    priority_weight = {pw}
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
                UPDATE hotspots
                SET critical_penalty_pc = {penalty},
                    priority_weight = {pw}
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
        print(f" updated {updated:>5d} hotspots → penalty {penalty:>3d}, weight {pw}  ({elapsed:.1f}s)")

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

    print("\nPriority weight distribution:")
    cur.execute("""
        SELECT priority_weight, COUNT(*) AS cnt
        FROM hotspots
        GROUP BY priority_weight
        ORDER BY priority_weight DESC
    """)
    for pw_val, cnt in cur.fetchall():
        print(f"  weight={pw_val}: {cnt:>6d}")

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
