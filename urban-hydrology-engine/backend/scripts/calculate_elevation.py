"""
calculate_elevation.py — Calculate ward-level elevation stats from SRTM 30m DEM.

Reads srtm_52_07.tif (covering 75-80°E, 25-30°N) and for each of the 290
Delhi wards, clips the raster, calculates slope, and stores results in
the ward_elevation table.

Usage:
    docker-compose exec backend python scripts/calculate_elevation.py
"""

import json
import os
import time

import numpy as np
import psycopg2
import rasterio
import rasterio.mask
from shapely.geometry import shape

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydro:hydro123@db:5432/hydrology"
)

SRTM_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "srtm", "srtm_52_07.tif")


def calculate_mean_slope(elevation_array, pixel_size_degrees=0.000833):
    """Calculate mean and max slope in degrees from an elevation array."""
    # Convert pixel size to metres (approximate for Delhi latitude ~28.65°)
    pixel_size_m = pixel_size_degrees * 111320 * np.cos(np.radians(28.65))

    elev = elevation_array.astype(float)
    # Replace nodata/-32768 with nan
    elev[elev < -1000] = np.nan

    if np.all(np.isnan(elev)) or elev.shape[0] < 3 or elev.shape[1] < 3:
        return 0.0, 0.0

    dy, dx = np.gradient(elev, pixel_size_m)
    slope_rad = np.arctan(np.sqrt(dx**2 + dy**2))
    slope_deg = np.degrees(slope_rad)

    mean_slope = float(np.nanmean(slope_deg))
    max_slope = float(np.nanmax(slope_deg))
    return mean_slope, max_slope


def slope_to_runoff_t(mean_slope):
    """Map mean slope (degrees) to runoff multiplier."""
    if mean_slope < 0.5:
        return 1.0
    elif mean_slope < 1.0:
        return 1.3
    elif mean_slope < 2.0:
        return 1.7
    elif mean_slope < 3.0:
        return 2.2
    elif mean_slope < 5.0:
        return 2.7
    else:
        return 3.5


def slope_to_terrain(mean_slope):
    """Map mean slope to terrain class label."""
    if mean_slope < 0.5:
        return "floodplain"
    elif mean_slope < 1.0:
        return "flat"
    elif mean_slope < 2.0:
        return "moderate"
    elif mean_slope < 3.0:
        return "steep"
    else:
        return "ridge"


def main():
    # Verify SRTM file exists
    if not os.path.exists(SRTM_PATH):
        print(f"ERROR: SRTM file not found at {SRTM_PATH}")
        print("Download it first — see the sprint guide.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ── Create table ────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ward_elevation (
            ward_id INTEGER PRIMARY KEY REFERENCES wards(id),
            mean_elevation FLOAT,
            min_elevation FLOAT,
            max_elevation FLOAT,
            elevation_range FLOAT,
            mean_slope FLOAT,
            max_slope FLOAT,
            runoff_t FLOAT,
            terrain_class VARCHAR
        );
    """)
    conn.commit()

    # Clear existing data (idempotent re-runs)
    cur.execute("DELETE FROM ward_elevation")
    conn.commit()

    # ── Fetch all wards with their geometry ─────────────────────
    cur.execute("""
        SELECT id, name, ST_AsGeoJSON(geom) as geom_json
        FROM wards
        ORDER BY id
    """)
    wards = cur.fetchall()
    n_wards = len(wards)

    if n_wards == 0:
        print("ERROR: No wards found.")
        return

    print(f"Calculating elevation statistics for {n_wards} Delhi wards...")
    print(f"SRTM file: {SRTM_PATH}\n")

    t0 = time.time()
    terrain_counts = {}
    all_elevations = []
    all_slopes = []
    success = 0
    defaults = 0

    with rasterio.open(SRTM_PATH) as src:
        for i, (ward_id, ward_name, geom_json) in enumerate(wards):
            geom = json.loads(geom_json)

            try:
                # Clip raster to ward boundary
                masked, transform = rasterio.mask.mask(
                    src, [geom], crop=True, nodata=-32768
                )
                elevation = masked[0].astype(float)

                # Replace nodata with nan
                elevation[elevation <= -32768] = np.nan

                valid = elevation[~np.isnan(elevation)]
                if len(valid) < 5:
                    raise ValueError(f"Only {len(valid)} valid pixels")

                mean_elev = float(np.nanmean(elevation))
                min_elev = float(np.nanmin(valid))
                max_elev = float(np.nanmax(valid))
                elev_range = max_elev - min_elev

                mean_slope, max_slope = calculate_mean_slope(elevation)
                runoff_t = slope_to_runoff_t(mean_slope)
                terrain = slope_to_terrain(mean_slope)

                all_elevations.append(mean_elev)
                all_slopes.append(mean_slope)
                success += 1

            except Exception as e:
                # Use defaults for wards with insufficient data
                mean_elev = 215.0
                min_elev = 215.0
                max_elev = 215.0
                elev_range = 0.0
                mean_slope = 1.0
                max_slope = 1.0
                runoff_t = 1.5
                terrain = "flat"
                defaults += 1
                if (i + 1) <= 10 or (i + 1) % 50 == 0:
                    print(f"  WARNING: {ward_name} — {e}, using defaults")

            cur.execute("""
                INSERT INTO ward_elevation
                    (ward_id, mean_elevation, min_elevation, max_elevation,
                     elevation_range, mean_slope, max_slope, runoff_t, terrain_class)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                ward_id, mean_elev, min_elev, max_elev,
                elev_range, mean_slope, max_slope, runoff_t, terrain,
            ))

            terrain_counts[terrain] = terrain_counts.get(terrain, 0) + 1

            if (i + 1) % 50 == 0:
                conn.commit()
                elapsed = time.time() - t0
                print(f"  Progress: {i + 1}/{n_wards} wards processed... ({elapsed:.1f}s)")

    conn.commit()
    elapsed = time.time() - t0

    # ── Summary ─────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM ward_elevation")
    total_rows = cur.fetchone()[0]

    print(f"\nDone. {total_rows} ward elevation rows ({success} computed, {defaults} defaults). Time: {elapsed:.1f}s\n")

    print("Terrain class distribution:")
    terrain_labels = {
        "floodplain": "runoff_t=1.0, Yamuna plain",
        "flat": "runoff_t=1.3",
        "moderate": "runoff_t=1.7",
        "steep": "runoff_t=2.2",
        "ridge": "runoff_t=3.5, Aravalli foothills",
    }
    for terrain in ["floodplain", "flat", "moderate", "steep", "ridge"]:
        count = terrain_counts.get(terrain, 0)
        label = terrain_labels.get(terrain, "")
        print(f"  {terrain:12s}: {count:>3d} wards  ({label})")

    if all_elevations:
        print(f"\nElevation range across Delhi: {min(all_elevations):.0f}m — {max(all_elevations):.0f}m")
    if all_slopes:
        print(f"Mean slope range: {min(all_slopes):.3f}° — {max(all_slopes):.3f}°")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
