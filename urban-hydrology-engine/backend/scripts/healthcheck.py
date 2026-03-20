#!/usr/bin/env python3
"""
healthcheck.py — Urban Hydrology Engine endpoint verifier + demo runner.

Usage:
  # Just verify all endpoints are alive
  python healthcheck.py

  # Full demo sequence: cloudburst → cycle → backtest (for live presentations)
  python healthcheck.py --demo

  # Point at a remote deployment
  python healthcheck.py --base https://your-app.onrender.com --demo
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import httpx
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

# ── Auto-load .env if present (for local Supabase runs) ──────────────────
def _load_dotenv():
    """Load .env from the backend directory if it exists."""
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        env_path = Path(".env")
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_load_dotenv()

# ── Defaults ─────────────────────────────────────────────────────────────
DEFAULT_BASE    = "http://127.0.0.1:8000"
API_KEY         = "hydro-mvp-secret-2026"
TIMEOUT_FAST    = 10
TIMEOUT_SLOW    = 60   # backtest / cycle can take 30-40s on remote

# ANSI colours (safe on Linux/Mac; gracefully stripped on Windows)
_NO_COLOUR = os.name == "nt" and "WT_SESSION" not in os.environ
def _c(code, text): return text if _NO_COLOUR else f"\033[{code}m{text}\033[0m"
GREEN  = lambda t: _c("32", t)
RED    = lambda t: _c("31", t)
YELLOW = lambda t: _c("33", t)
CYAN   = lambda t: _c("36", t)
BOLD   = lambda t: _c("1",  t)
DIM    = lambda t: _c("2",  t)


# ── Endpoint definitions ──────────────────────────────────────────────────
ENDPOINTS = [
    # (method, path, auth, timeout, description, expect_keys)
    ("GET",  "/health",              False, TIMEOUT_FAST,
     "DB health",              ["status", "db"]),

    ("GET",  "/city/bounds",         False, TIMEOUT_FAST,
     "City bounds",            ["city_lat", "ward_count"]),

    ("GET",  "/map/state",           False, TIMEOUT_SLOW,
     "Ward GeoJSON (290 wards)", ["type", "features"]),

    ("GET",  "/map/infrastructure",  False, TIMEOUT_FAST,
     "Infrastructure GeoJSON", ["type", "features"]),

    ("GET",  "/map/elevation",       False, TIMEOUT_FAST,
     "Elevation choropleth",   ["type", "features"]),

    ("GET",  "/weather/status",      False, TIMEOUT_FAST,
     "Weather status",         []),

    ("GET",  "/forecast/6hour",      False, TIMEOUT_FAST,
     "6-hour forecast",        ["risk_level"]),

    ("GET",  "/cycle/latest",        False, TIMEOUT_FAST,
     "Last cycle timestamp",   []),

    ("GET",  "/readiness/summary",   False, TIMEOUT_FAST,
     "Readiness summary",      ["total_wards", "red_wards"]),

    ("GET",  "/readiness/scores",    False, TIMEOUT_SLOW,
     "Readiness scores (all wards)", ["wards", "summary"]),

    ("GET",  "/backtest/2023/status",False, TIMEOUT_FAST,
     "Backtest job status",    ["status"]),

    ("GET",  "/yamuna/status",       False, TIMEOUT_FAST,
     "Yamuna advance warning", ["alert_level"]),

    ("GET",  "/ssi",                 False, TIMEOUT_FAST,
     "System Saturation Index", ["ssi", "level"]),

    ("GET",  "/export/summary",      False, TIMEOUT_SLOW,
     "Printable ward summary",  []),

    ("GET",  "/ward/1/detail",       False, TIMEOUT_FAST,
     "Ward detail (ward 1)",   ["ward_id"]),
]


# ── Runner ────────────────────────────────────────────────────────────────

def check_endpoints(base: str, client: httpx.Client) -> list[dict]:
    results = []
    print(f"\n{BOLD('Endpoint health check')}  →  {CYAN(base)}\n")
    print(f"{'STATUS':<8} {'PATH':<35} {'TIME':>6}  {'NOTE'}")
    print("─" * 72)

    for method, path, auth, timeout, desc, expect_keys in ENDPOINTS:
        headers = {"X-API-Key": API_KEY} if auth else {}
        t0 = time.perf_counter()
        try:
            resp = client.request(method, path, headers=headers, timeout=timeout)
            elapsed = time.perf_counter() - t0
            ok = resp.status_code < 400

            if ok:
                try:
                    body = resp.json()
                    missing = [k for k in expect_keys if k not in body]
                    if missing:
                        ok = False
                        note = f"missing keys: {missing}"
                    else:
                        # Extra info
                        if path == "/health":
                            note = f"db={body.get('db','?')}"
                        elif path == "/readiness/summary":
                            note = (f"{body.get('total_wards','?')} wards  "
                                    f"red={body.get('red_wards','?')}  "
                                    f"days_left={body.get('days_to_monsoon','?')}")
                        elif path == "/map/state":
                            note = f"{len(body.get('features',[]))} ward features"
                        elif path == "/yamuna/status":
                            note = (f"{body.get('alert_level','?')}  "
                                    f"{body.get('discharge_cusecs','?')} cusecs  "
                                    f"src={body.get('source','?')[:20]}")
                        elif path == "/backtest/2023/status":
                            note = f"status={body.get('status','?')}"
                        elif path == "/forecast/6hour":
                            note = f"risk={body.get('risk_level','?')}  {body.get('total_6h_mm','?')}mm"
                        else:
                            note = desc
                except Exception:
                    note = f"HTTP {resp.status_code} (non-JSON)"
            else:
                note = f"HTTP {resp.status_code}"

        except httpx.TimeoutException:
            elapsed = time.perf_counter() - t0
            ok    = False
            note  = f"TIMEOUT after {elapsed:.1f}s"
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            ok    = False
            note  = str(exc)[:60]

        status_str = GREEN("  PASS") if ok else RED("  FAIL")
        print(f"{status_str}   {path:<35} {elapsed*1000:>5.0f}ms  {DIM(note)}")
        results.append({"path": path, "ok": ok, "elapsed": elapsed, "note": note})

    passed = sum(1 for r in results if r["ok"])
    total  = len(results)
    colour = GREEN if passed == total else (YELLOW if passed >= total * 0.8 else RED)
    print("─" * 72)
    print(f"{colour(f'{passed}/{total} endpoints healthy')}\n")
    return results


def run_demo(base: str, client: httpx.Client):
    """
    Full live demo sequence for presentations:
      1. Show current readiness summary
      2. Simulate a cloudburst (ingest rain + run cycle)
      3. Show triggered wards
      4. Trigger 2023 backtest and poll until complete
      5. Print precision/recall
    """
    print(f"\n{BOLD('Demo sequence')}  →  {CYAN(base)}\n")

    # ── 1. Readiness snapshot ────────────────────────────────────────
    _section("Step 1 — Pre-Monsoon Readiness Snapshot")
    r = client.get("/readiness/summary", timeout=TIMEOUT_SLOW)
    if r.status_code == 200:
        s = r.json()
        days = s.get("days_to_monsoon", "?")
        print(f"  {CYAN(str(days))} days until monsoon (June 15)")
        print(f"  {RED(str(s.get('red_wards','?')))} critical  "
              f"{YELLOW(str(s.get('amber_wards','?')))} at-risk  "
              f"{GREEN(str(s.get('green_wards','?')))} ready")
        ft = s.get("flood_type_counts", {})
        if ft:
            print(f"  Flood types: " + "  ".join(f"{k}={v}" for k, v in ft.items()))
    else:
        print(f"  {RED('FAILED')} HTTP {r.status_code}")

    # ── 2. Yamuna status ─────────────────────────────────────────────
    _section("Step 2 — Yamuna / Hathnikund Status")
    r = client.get("/yamuna/status", timeout=TIMEOUT_FAST)
    if r.status_code == 200:
        y = r.json()
        lvl = y.get("alert_level", "?")
        col = RED if lvl in ("DANGER","EXTREME","WARNING") else (YELLOW if lvl == "WATCH" else GREEN)
        print(f"  Alert: {col(lvl)}")
        print(f"  Discharge: {y.get('discharge_cusecs', '?'):,} cusecs  "
              f"→  predicted Delhi stage: {y.get('predicted_delhi_stage_m','?')}m")
        print(f"  ETA: {y.get('eta_delhi','?')}  ({y.get('travel_time_hours','?')}h)")
        print(f"  Source: {DIM(y.get('source','?'))}")
    else:
        print(f"  {RED('FAILED')} HTTP {r.status_code}")

    # ── 3. Simulate cloudburst ────────────────────────────────────────
    _section("Step 3 — Cloudburst Simulation")
    bounds_r = client.get("/city/bounds", timeout=TIMEOUT_FAST)
    if bounds_r.status_code != 200:
        print(f"  {RED('Cannot get city bounds — skipping')}")
    else:
        b = bounds_r.json()
        lat, lon = b.get("city_lat", 28.65), b.get("city_lon", 77.22)
        d = 0.15
        poly = {"type": "Polygon", "coordinates": [
            [[lon-d, lat-d], [lon+d, lat-d],
             [lon+d, lat+d], [lon-d, lat+d], [lon-d, lat-d]]
        ]}
        rain_r = client.post(
            "/ingest/rain",
            json={"geojson_polygon": poly, "intensity_r": 7.5},
            headers={"X-API-Key": API_KEY},
            timeout=TIMEOUT_FAST,
        )
        if rain_r.status_code == 200:
            rj = rain_r.json()
            print(f"  Rain ingested → {rj.get('hotspots_in_polygon','?')} hotspots hit")
        else:
            print(f"  {YELLOW('Rain ingest returned')} HTTP {rain_r.status_code}")

        # Run cycle
        print("  Running scoring cycle…")
        t0 = time.perf_counter()
        cycle_r = client.post("/run/cycle",
                              headers={"X-API-Key": API_KEY},
                              timeout=TIMEOUT_SLOW)
        elapsed = time.perf_counter() - t0
        if cycle_r.status_code == 200:
            cj = cycle_r.json()
            triggered = cj.get("wards_triggered", 0)
            safe      = cj.get("safe_wards", 0)
            print(f"  Cycle complete in {elapsed:.1f}s")
            print(f"  {RED(str(triggered))} wards triggered  "
                  f"{GREEN(str(safe))} wards safe")
            # Show top 3 dispatches
            for d in cj.get("dispatches", [])[:3]:
                score = d.get("ws_score", "?")
                print(f"    ↳ {d.get('ward_name','?'):30s}  score={score:.1f}  "
                      f"{DIM(d.get('dispatch_message','')[:60])}")
        else:
            print(f"  {RED('Cycle failed')} HTTP {cycle_r.status_code}")

    # ── 4. 2023 Backtest ──────────────────────────────────────────────
    _section("Step 4 — 2023 Flood Replay Backtest")

    # Check if result already cached
    st_r = client.get("/backtest/2023/status", timeout=TIMEOUT_FAST)
    st   = st_r.json().get("status", "idle") if st_r.status_code == 200 else "idle"

    if st == "complete":
        print("  Using cached 2023 result.")
    else:
        print("  Triggering 2023 replay (background job)…")
        trig_r = client.post("/backtest/2023",
                             headers={"X-API-Key": API_KEY},
                             timeout=TIMEOUT_FAST)
        if trig_r.status_code not in (200, 202):
            print(f"  {RED('Trigger failed')} HTTP {trig_r.status_code}")
            return

        # Poll until complete
        print("  Waiting", end="", flush=True)
        for _ in range(90):   # max 90s
            time.sleep(1)
            st_r = client.get("/backtest/2023/status", timeout=TIMEOUT_FAST)
            if st_r.status_code != 200:
                break
            st = st_r.json().get("status", "running")
            if st in ("complete", "error"):
                break
            print(".", end="", flush=True)
        print()

    if st != "complete":
        print(f"  {RED('Backtest did not complete')}: status={st}")
        return

    res_r = client.get("/backtest/2023/result", timeout=TIMEOUT_FAST)
    if res_r.status_code != 200:
        print(f"  {RED('Cannot fetch result')} HTTP {res_r.status_code}")
        return

    res = res_r.json()
    print(f"  {BOLD('2023 Replay Results')}")
    print(f"  Reference flooded wards : {res.get('n_reference_flooded','?')}")
    print(f"  Model predicted flooded : {res.get('n_predicted_flooded','?')}")
    print(f"  True positives  : {GREEN(str(res.get('true_positives','?')))}")
    print(f"  Missed (FN)     : {RED(str(res.get('false_negatives','?')))}")
    print(f"  Over-pred (FP)  : {YELLOW(str(res.get('false_positives','?')))}")
    print(f"  Precision : {CYAN(str(res.get('precision','?')) + '%')}")
    print(f"  Recall    : {CYAN(str(res.get('recall','?')) + '%')}")
    print(f"  F1 Score  : {CYAN(str(res.get('f1_score','?')) + '%')}")

    print(f"\n{GREEN('Demo sequence complete.')}  "
          f"Open {CYAN(base)} in your browser for the live dashboard.\n")


def _section(title: str):
    print(f"\n{BOLD('── ' + title)}")


# ── Entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Hydrology Engine health check + demo")
    parser.add_argument("--base",  default=DEFAULT_BASE, help="Base URL of the API")
    parser.add_argument("--demo",  action="store_true",  help="Run full demo sequence")
    parser.add_argument("--key",   default=API_KEY,      help="API key for write endpoints")
    parser.add_argument("--local", action="store_true",
                        help="Shortcut for --base http://127.0.0.1:8000 (local uvicorn)")
    args = parser.parse_args()

    # --local shortcut
    if args.local:
        args.base = "http://127.0.0.1:8000"

    # Auto-detect Render URL from environment if base not overridden
    if args.base == DEFAULT_BASE and not args.local:
        render_url = os.environ.get("RENDER_EXTERNAL_URL", "")
        if render_url:
            args.base = render_url
            print(f"  Auto-detected Render URL: {CYAN(render_url)}")

    base = args.base.rstrip("/")

    print(f"\n{BOLD('Urban Hydrology Engine — Health Check')}")
    print(f"  Target : {CYAN(base)}")
    print(f"  Time   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    with httpx.Client(base_url=base) as client:
        results = check_endpoints(base, client)
        failed  = [r for r in results if not r["ok"]]

        if args.demo:
            run_demo(base, client)

    if failed:
        print(f"{RED('Failed endpoints:')}")
        for r in failed:
            print(f"  {r['path']}  —  {r['note']}")
        sys.exit(1)
    else:
        print(f"{GREEN('All endpoints healthy.')}")
        sys.exit(0)


if __name__ == "__main__":
    main()
