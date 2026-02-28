"""
seed_history.py — Seed dispatch_runs from real Delhi 2024 monsoon rainfall.

Reads backend/data/delhi_monsoon_2024.json (Open-Meteo historical API)
and generates 290 dispatch_run rows per day x 122 days = 35,380 rows
using actual daily rainfall totals (Jun 1 - Sep 30, 2024).

Key real events:
  Sep 12: 100.9mm - worst single-day cloudburst of 2024
  Jul 31:  97.6mm - peak July event
  Sep 13:  54.6mm - back-to-back flooding
  Aug  9:  35.7mm - mid-monsoon event

Usage:
  docker-compose exec backend python scripts/seed_history.py
"""

import json
import os
import random
import time
from datetime import datetime

import psycopg2

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://hydro:hydro123@db:5432/hydrology"
)

DATA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "delhi_monsoon_2024.json"
)


# ---------------------------------------------------------------------------
# Rainfall -> ward trigger mapping
# ---------------------------------------------------------------------------

def rainfall_to_triggered(rain_mm):
    """Map daily rainfall (mm) -> number of triggered wards (out of 290)."""
    if rain_mm < 2:
        return random.randint(0, 5)
    elif rain_mm < 10:
        return random.randint(5, 20)
    elif rain_mm < 25:
        return random.randint(20, 60)
    elif rain_mm < 50:
        return random.randint(60, 120)
    elif rain_mm < 75:
        return random.randint(120, 180)
    elif rain_mm < 100:
        return random.randint(160, 220)
    else:
        return random.randint(220, 275)


def triggered_to_critical(triggered, rain_mm):
    """Map triggered count -> critical count (subset)."""
    if triggered == 0:
        return 0
    if rain_mm < 25:
        return random.randint(0, max(1, int(triggered * 0.10)))
    elif rain_mm < 50:
        lo = int(triggered * 0.15)
        hi = int(triggered * 0.25)
        return random.randint(lo, max(lo, hi))
    else:
        lo = int(triggered * 0.25)
        hi = int(triggered * 0.40)
        return random.randint(lo, max(lo, hi))


def rain_to_intensity_label(rain_mm):
    """Classify rainfall intensity."""
    if rain_mm < 2:
        return "trace"
    elif rain_mm < 10:
        return "light"
    elif rain_mm < 25:
        return "moderate"
    elif rain_mm < 50:
        return "heavy"
    elif rain_mm < 75:
        return "very heavy"
    elif rain_mm < 100:
        return "extremely heavy"
    else:
        return "cloudburst"


def main():
    # -- Load monsoon data ---------------------------------------------------
    if not os.path.exists(DATA_PATH):
        print(f"ERROR: Monsoon data not found at {DATA_PATH}")
        print("Download it first:")
        print('  curl -s "https://archive-api.open-meteo.com/v1/archive?..."'
              ' -o backend/data/delhi_monsoon_2024.json')
        return

    with open(DATA_PATH) as f:
        meteo = json.load(f)

    dates = meteo["daily"]["time"]
    rain_values = [r if r is not None else 0.0 for r in meteo["daily"]["rain_sum"]]

    print("Seeding real 2024 Delhi monsoon data...")
    print(f"  Source: Open-Meteo historical API (lat={meteo['latitude']:.3f}, "
          f"lon={meteo['longitude']:.3f}, elev={meteo['elevation']}m)")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # -- Clear old historical data (keep last 2 hours of live test data) -----
    cur.execute("""
        DELETE FROM dispatch_runs
        WHERE created_at < NOW() - INTERVAL '2 hours'
    """)
    cleared = cur.rowcount
    conn.commit()
    print(f"  Cleared {cleared:,} old historical rows.")

    # -- Fetch all ward data -------------------------------------------------
    cur.execute("SELECT id, name FROM wards ORDER BY id")
    wards = cur.fetchall()
    ward_ids = [w[0] for w in wards]
    ward_names = {w[0]: w[1] for w in wards}
    n_wards = len(ward_ids)

    if n_wards == 0:
        print("ERROR: No wards found. Run import_delhi_wards.py first.")
        return

    print(f"  Fetched {len(dates)} days from Open-Meteo (Jun 1 - Sep 30, 2024)")
    print(f"  Wards: {n_wards}\n")
    print("Processing...")

    t0 = time.time()
    total_inserted = 0
    day_stats = []
    monthly_triggered = {}

    for day_idx, (date_str, rain_mm) in enumerate(zip(dates, rain_values)):
        # Parse date and create realistic timestamp
        base_date = datetime.strptime(date_str, "%Y-%m-%d")
        # Monsoon rains peak 14:00-20:00 IST = 08:30-14:30 UTC
        hour = random.randint(8, 14)
        minute = random.randint(0, 59)
        created_at = base_date.replace(hour=hour, minute=minute, second=0)

        # Calculate ward distribution
        n_triggered = rainfall_to_triggered(rain_mm)
        n_critical = triggered_to_critical(n_triggered, rain_mm)
        n_dispatched = n_triggered - n_critical
        n_safe = n_wards - n_triggered

        intensity = rain_to_intensity_label(rain_mm)

        # Pick non-overlapping ward sets
        shuffled = random.sample(ward_ids, n_wards)
        critical_ids = set(shuffled[:n_critical])
        dispatched_ids = set(shuffled[n_critical:n_triggered])
        # safe_ids = rest

        rows = []
        for wid in ward_ids:
            wname = ward_names[wid]

            if wid in critical_ids:
                ws_score = round(random.uniform(-800, -50), 1)
                pumps = random.randint(10, 15)
                locs = random.randint(5, 10)
                dispatch_msg = (
                    f"PMRS {ws_score:.1f} -- Auto-dispatch: {pumps} pumps "
                    f"to {locs} locations in {wname} CRITICAL"
                )
                result_json = json.dumps({
                    "dispatch_message": dispatch_msg,
                    "pumps_total": pumps,
                    "rain_mm": rain_mm,
                    "intensity_label": intensity,
                    "source": "real_2024_monsoon",
                })
                status = "critical"

            elif wid in dispatched_ids:
                ws_score = round(random.uniform(-49, 69), 1)
                pumps = random.randint(3, 10)
                locs = random.randint(2, min(pumps, 8))
                dispatch_msg = (
                    f"PMRS {ws_score:.1f} -- Dispatched {pumps} pumps "
                    f"to {locs} locations in {wname}"
                )
                result_json = json.dumps({
                    "dispatch_message": dispatch_msg,
                    "pumps_total": pumps,
                    "rain_mm": rain_mm,
                    "intensity_label": intensity,
                    "source": "real_2024_monsoon",
                })
                status = "dispatched"

            else:
                ws_score = round(random.uniform(75, 100), 1)
                result_json = json.dumps({
                    "message": "Ward score nominal",
                    "rain_mm": rain_mm,
                    "source": "real_2024_monsoon",
                })
                status = "safe"

            rows.append((wid, ws_score, status, result_json, created_at))

        # Batch insert all 290 rows for this day
        cur.executemany(
            """
            INSERT INTO dispatch_runs (ward_id, ws_score, status, result_json, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            rows,
        )
        conn.commit()
        total_inserted += len(rows)

        # Track stats
        month = date_str[:7]
        monthly_triggered.setdefault(month, []).append(n_triggered)

        day_stats.append({
            "date": date_str,
            "rain_mm": rain_mm,
            "intensity": intensity,
            "triggered": n_triggered,
            "critical": n_critical,
        })

        # Print notable days + progress every 20 days
        is_notable = rain_mm >= 25 or n_triggered >= 100
        is_progress = (day_idx + 1) % 20 == 0
        if is_notable or is_progress:
            marker = " <-- major event" if rain_mm >= 50 else ""
            print(
                f"  {date_str} | {rain_mm:6.1f}mm | {intensity:<16s} | "
                f"{n_triggered:3d} triggered, {n_critical:3d} critical{marker}"
            )

    elapsed = time.time() - t0
    conn.close()

    # -- Summary -------------------------------------------------------------
    peak = max(day_stats, key=lambda d: d["triggered"])
    rainy_days = sum(1 for d in day_stats if d["rain_mm"] > 0.1)
    heavy_plus = sum(1 for d in day_stats if d["rain_mm"] >= 25)
    cloudburst_days = sum(1 for d in day_stats if d["rain_mm"] >= 100)
    dry_days = sum(1 for d in day_stats if d["rain_mm"] < 2)
    total_rain = sum(d["rain_mm"] for d in day_stats)

    print(f"\nDone. Inserted {total_inserted:,} rows ({len(dates)} days x {n_wards} wards). "
          f"Time: {elapsed:.1f}s")
    print(f"Date range: {dates[0]} to {dates[-1]}")
    print(f"Total rainfall: {total_rain:.1f}mm\n")

    print("Real 2024 monsoon highlights:")
    print(f"  Peak day:          {peak['date']} -- "
          f"{peak['triggered']} wards triggered ({peak['triggered']/n_wards*100:.1f}%)")

    # Worst month by avg triggered/day
    worst_month = max(monthly_triggered.items(),
                      key=lambda x: sum(x[1]) / len(x[1]))
    avg_worst = sum(worst_month[1]) / len(worst_month[1])
    month_name = datetime.strptime(worst_month[0], "%Y-%m").strftime("%B")
    print(f"  Worst month:       {month_name} -- avg {avg_worst:.0f} triggered/day")
    print(f"  Total cloudburst days: {cloudburst_days}")
    print(f"  Total heavy+ days: {heavy_plus}")
    print(f"  Total rainy days:  {rainy_days}")
    print(f"  Dry days (< 2mm):  {dry_days}")


if __name__ == "__main__":
    main()
