"""
auto_seed.py — Automatically seed an empty database on first startup.

Runs the import/seed scripts in order if the `wards` table is empty.
Designed for Render (or any fresh deployment) so no manual steps are needed.

Skips elevation scripts (calculate_elevation, update_hotspot_elevation)
because they require SRTM .tif files not included in the Docker image.
"""

import os
import sys
import time

import psycopg2

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://hydro:hydro123@localhost:5432/hydrology",
)


def _db_is_empty() -> bool:
    """Return True if the wards table has zero rows (or doesn't exist)."""
    # Handle Render's postgres:// prefix
    import re
    url = re.sub(r"^postgres(ql)?://", "postgresql://", DATABASE_URL)
    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        cur.execute(
            "SELECT EXISTS ("
            "  SELECT FROM information_schema.tables WHERE table_name = 'wards'"
            ")"
        )
        table_exists = cur.fetchone()[0]
        if not table_exists:
            cur.close()
            conn.close()
            return True
        cur.execute("SELECT COUNT(*) FROM wards")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count == 0
    except Exception as exc:
        print(f"[auto_seed] Could not check DB: {exc}")
        return False


def run_seed():
    """Execute the 4 seed scripts in order."""
    # Ensure scripts/ directory is importable
    scripts_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
    scripts_dir = os.path.abspath(scripts_dir)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    # Fix DATABASE_URL for psycopg2 (handle postgres:// from Render)
    import re
    fixed_url = re.sub(r"^postgres(ql)?://", "postgresql://", DATABASE_URL)
    os.environ["DATABASE_URL"] = fixed_url

    steps = [
        ("import_delhi_wards",        "Import 290 real Delhi ward boundaries + hotspots"),
        ("import_osm_infrastructure", "Import OSM hospitals, substations, fire stations"),
        ("update_hotspot_penalties",  "Calculate hotspot penalties from infrastructure proximity"),
        ("seed_history",              "Seed 2024 monsoon dispatch history (122 days)"),
    ]

    t0 = time.time()
    print("\n" + "=" * 60)
    print("AUTO-SEED: Database is empty — running first-time setup")
    print("=" * 60)

    for module_name, description in steps:
        print(f"\n>>> {description}")
        print("-" * 60)
        try:
            mod = __import__(module_name)
            mod.main()
            print(f"    OK ({module_name})")
        except Exception as exc:
            print(f"    WARN: {module_name} failed: {exc}")
            # Continue with remaining scripts — partial data is better than none

    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print(f"AUTO-SEED COMPLETE — {elapsed:.1f}s")
    print("=" * 60 + "\n")


def auto_seed_if_empty():
    """Main entry point: seed only if DB is empty."""
    if _db_is_empty():
        run_seed()
    else:
        print("[auto_seed] Database already has data — skipping seed.")
