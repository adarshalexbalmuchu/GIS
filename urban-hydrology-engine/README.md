# Urban Hydrology Engine — Delhi Flood Intelligence System

Real-time flood-risk scoring, Pre-Monsoon Readiness Score, and pump-dispatch optimisation for Delhi's 290 municipal wards.

**Stack:** FastAPI · PostgreSQL/PostGIS (Supabase) · Leaflet.js · Chart.js  
**Deployment:** Render (free tier) + Supabase (free tier) — **zero cost**  
**Local dev:** Python + any terminal — **no Docker required**

---

## Zero to running — 5 minutes

### Step 1 — Supabase (free, no credit card)

1. Go to [supabase.com](https://supabase.com) → New project
2. Wait ~2 minutes for provisioning
3. **Project Settings → Database → URI tab** → copy the connection string:
   ```
   postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-REF].supabase.co:5432/postgres
   ```
   > Use the **URI** tab, NOT the "Connection pooling" tab

### Step 2 — Render (free, no credit card)

1. [render.com](https://render.com) → New → **Blueprint** → connect this repo
2. Render detects `render.yaml` automatically
3. Set two environment variables in the Render dashboard:
   - `DATABASE_URL` → your Supabase URI from Step 1
   - `OWM_API_KEY` → free key from [openweathermap.org](https://openweathermap.org/api) *(optional)*
4. Deploy

**First deploy takes ~5 minutes** — seeds 290 wards, 32,826 hotspots, and 122 days of monsoon history automatically. All subsequent deploys are instant.

### Step 3 — Open the dashboard

```
https://YOUR-APP-NAME.onrender.com
```

---

## Local development

No Docker. Needs Python 3.11+ and your Supabase URI.

**One-time setup:**
```bash
cd urban-hydrology-engine
pip install -r backend/requirements.txt

# Windows
copy backend\.env.example backend\.env

# Mac/Linux
cp backend/.env.example backend/.env
```

Edit `backend/.env` — set `DATABASE_URL` to your Supabase URI. Everything else has defaults.

**Start the app:**

Windows (PowerShell):
```powershell
.\run_local.ps1
```

Mac / Linux:
```bash
cd backend
export $(grep -v '#' .env | xargs)
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Open `http://localhost:8000`

---

## Verify everything works

```bash
# Quick health check
python backend/scripts/healthcheck.py --local

# Full demo — readiness → Yamuna → cloudburst → cycle → 2023 backtest
python backend/scripts/healthcheck.py --local --demo

# Against live Render deployment
python backend/scripts/healthcheck.py --base https://YOUR-APP.onrender.com --demo
```

---

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | **Yes** | — | Supabase PostgreSQL URI |
| `OWM_API_KEY` | No | — | OpenWeatherMap key (live rain polling) |
| `API_SECRET_KEY` | No | `hydro-mvp-secret-2026` | Key for write endpoints |
| `CITY_LAT` | No | `28.65` | Delhi centre latitude |
| `CITY_LON` | No | `77.22` | Delhi centre longitude |
| `RAIN_POLL_INTERVAL_SECONDS` | No | `600` | Weather polling interval (seconds) |
| `TILESERV_URL` | No | `` (empty) | Vector tile server — leave empty on free tier |

---

## API reference

Write endpoints require header: `X-API-Key: hydro-mvp-secret-2026`

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/health` | No | DB connectivity check |
| GET | `/map/state` | No | 290 wards as GeoJSON with live scores |
| GET | `/map/infrastructure` | No | Hospitals, substations, fire stations |
| GET | `/map/elevation` | No | Ward elevation choropleth |
| GET | `/map/floodline` | No | Yamuna flood zone classification |
| GET | `/readiness/scores` | No | Pre-Monsoon Readiness Score — all wards |
| GET | `/readiness/summary` | No | Aggregate stats + days to monsoon |
| GET | `/readiness/ward/{id}` | No | Single ward readiness detail |
| GET | `/yamuna/status` | No | Hathnikund discharge + advance warning |
| GET | `/ssi` | No | System Saturation Index |
| GET | `/forecast/6hour` | No | 6-hour rainfall forecast |
| GET | `/weather/status` | No | Current weather |
| GET | `/backtest/2023/status` | No | 2023 replay job progress |
| GET | `/backtest/2023/result` | No | 2023 replay results |
| GET | `/export/ward/{id}` | No | Printable HTML ward report |
| GET | `/export/summary` | No | Printable all-ward league table |
| GET | `/ward/{id}/detail` | No | Ward score history + infrastructure |
| POST | `/run/cycle` | **Yes** | Run PMRS scoring + LP dispatch |
| POST | `/ingest/rain` | **Yes** | Submit rainfall event |
| POST | `/backtest/2023` | **Yes** | Start 2023 flood replay |
| POST | `/reset` | **Yes** | Reset all events |

---

## Troubleshooting

**Map is empty after deploy**  
Still seeding. Wait 3–5 minutes and refresh. Check Render logs for `AUTO-SEED COMPLETE`.

**`DATABASE_URL` error**  
Use the **URI** tab in Supabase, not the pooler. The app handles both `postgresql://` and `postgres://` prefixes automatically.

**`Connection refused` on healthcheck**  
App isn't running. Start it first with `.\run_local.ps1`.

**Render deploy fails at GDAL**  
Transient build issue. Trigger a manual redeploy from the Render dashboard.

**Backtest takes 20–40 seconds**  
Expected. It scores 290 wards against 6 rain events. The progress bar updates live via WebSocket. Don't refresh.

---

## Cost

| Service | Monthly cost |
|---|---|
| Render (free tier) | $0 |
| Supabase (free tier, 500MB) | $0 |
| Open-Meteo | $0 |
| OpenWeatherMap (free tier) | $0 |
| **Total** | **$0** |

Free Render tier sleeps after 15 minutes idle — first request after sleep takes ~30 seconds. Fine for demos. For always-on: Render Starter is $7/month.
