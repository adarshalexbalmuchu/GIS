# Urban Hydrology Engine

**Real-time flood-risk scoring and pump-dispatch optimization for Delhi's 290 municipal wards.**

Built with FastAPI, PostGIS, PuLP linear programming, and real 2024 monsoon data. A single-page Leaflet dashboard provides live ward-level risk maps, automated dispatch recommendations, and historical monsoon analytics.

---

## Features

| Feature | Description |
|---|---|
| **PMRS Scoring** | Per-ward flood risk scored via rainfall intensity, drainage capacity, infrastructure proximity, and terrain elevation |
| **LP Dispatch** | PuLP linear program optimizes pump allocation across triggered wards every cycle |
| **290 Real Wards** | Delhi ward boundaries from Datameet, with zone classification |
| **32,826 Hotspots** | Flood-risk grid generated from ward geometries |
| **1,235 Critical Infrastructure** | Hospitals, substations, fire stations from OpenStreetMap — proximity penalties in scoring |
| **SRTM Elevation** | 90m DEM terrain classification (floodplain to ridge) with runoff multipliers |
| **Real Monsoon Data** | 122 days of 2024 monsoon rainfall from Open-Meteo (922.9mm total) |
| **Live Weather** | OpenWeatherMap polling with auto-triggered scoring cycles |
| **Vector Tiles** | pg_tileserv serves hotspot layers as Mapbox Vector Tiles |
| **Historical Analytics** | Stacked bar chart with event annotations, worst-ward rankings, season comparison |

---

## Architecture

```
+------------------------------------------------------------------+
|                        Browser (Leaflet.js)                       |
|  +-----------+  +----------+  +----------+  +-----------------+  |
|  | Ward Map  |  | Hotspots |  | Controls |  | History Panel   |  |
|  | GeoJSON   |  | VT/PBF   |  | Console  |  | Chart.js        |  |
|  +-----+-----+  +-----+----+  +-----+----+  +-------+---------+  |
+---------+------------+--------------+----------------+------------+
          |            |              |                |
          v            v              v                v
+-------------------------------------------------------------------+
|                    FastAPI Backend (:8000)                         |
|                                                                   |
|  /map/state --- /run/cycle --- /history/* --- /weather/status      |
|  /map/elevation  /ingest/rain    /map/infrastructure  /reset      |
|                  /ingest/sensor  /city/bounds     /cycle/latest   |
|                                                                   |
|  +--------------+  +--------------+  +----------------+           |
|  |  scoring.py  |  | dispatch_lp  |  |  weather.py    |           |
|  |  PMRS algo   |  | PuLP LP      |  |  OWM + sched   |           |
|  +------+-------+  +------+-------+  +-------+--------+           |
+----------+----------------+------------------+--------------------+
           |                |                  |
           v                v                  v
+-------------------------------------------------------------------+
|              PostgreSQL 15 + PostGIS 3.3                           |
|                                                                   |
|  wards (290)  hotspots (32,826)  dispatch_runs  rain_events       |
|  sensor_events  critical_infrastructure (1,235)  ward_elevation   |
|                                                                   |
|     <---- pg_tileserv (:7800) --- serves MVT tiles                |
+-------------------------------------------------------------------+
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Frontend** | Leaflet.js 1.9.4, Chart.js 4.4, Leaflet.VectorGrid | Map rendering, charts, vector tiles |
| **API** | FastAPI 0.111, Uvicorn, Pydantic 2.7 | Async REST API with auth |
| **Database** | PostgreSQL 15, PostGIS 3.3, asyncpg | Spatial queries, GeoJSON generation |
| **Tile Server** | pg_tileserv | Mapbox Vector Tiles from PostGIS |
| **Optimization** | PuLP 2.8 | Linear programming for pump dispatch |
| **Geospatial** | GeoAlchemy2, Shapely, GDAL, rasterio | Spatial ORM, geometry ops, DEM processing |
| **Weather** | OpenWeatherMap API, APScheduler | Live rainfall polling, auto-cycle triggers |
| **Containerization** | Docker Compose 3.9 | 3-service stack (db, backend, tileserv) |

---

## Database Schema

| Table | Rows | Description |
|---|---|---|
| `wards` | 290 | Delhi ward polygons with zone, centroid, area |
| `hotspots` | 32,826 | Flood-risk grid cells with capacity (0-100) |
| `rain_events` | dynamic | Rainfall polygon events with intensity |
| `sensor_events` | dynamic | IoT capacity-change deltas per hotspot |
| `dispatch_runs` | ~37,000+ | LP optimization results per ward per cycle |
| `critical_infrastructure` | 1,235 | Hospitals (871), substations (324), fire stations (40) |
| `ward_elevation` | 290 | SRTM elevation stats, terrain class, runoff multiplier |

---

## API Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/health` | No | Health check |
| `GET` | `/map/state` | No | All wards as GeoJSON with scores + status |
| `GET` | `/map/infrastructure` | No | Critical infrastructure points as GeoJSON |
| `GET` | `/map/elevation` | No | Ward elevation choropleth as GeoJSON |
| `GET` | `/city/bounds` | No | City center, bounds, ward/hotspot counts |
| `GET` | `/weather/status` | No | Current OWM weather + rain state |
| `GET` | `/cycle/latest` | No | Timestamp of last scoring cycle |
| `POST` | `/run/cycle` | API key | Run PMRS scoring + LP dispatch |
| `POST` | `/ingest/rain` | API key | Submit a rainfall polygon event |
| `POST` | `/ingest/sensor` | API key | Submit a sensor capacity delta |
| `POST` | `/reset` | API key | Reset all events + dispatch history |
| `GET` | `/history/timeline` | No | Daily dispatch breakdown (stacked bar data) |
| `GET` | `/history/worst-wards` | No | Top wards by trigger count |
| `GET` | `/history/summary` | No | Aggregate stats for a time period |
| `GET` | `/history/seasons` | No | Available monsoon seasons in DB |

### Query Parameters (History)

- `days` — Number of days to look back (default: 30)
- `year` — Monsoon year filter, e.g. `2024` (overrides days to Jun 1-Sep 30)
- `limit` — Max wards for worst-wards endpoint (default: 10)

---

## Quick Start

### Prerequisites

- Docker and Docker Compose
- ~2 GB disk (PostGIS image + SRTM data)

### 1. Start Services

```bash
docker compose up --build -d
```

Three containers launch: `db` (PostGIS), `backend` (FastAPI), `tileserv` (pg_tileserv).

### 2. Seed the Database

```bash
# Import 290 Delhi ward boundaries
docker compose exec backend python scripts/import_delhi_wards.py

# Import OSM infrastructure (hospitals, substations, fire stations)
docker compose exec backend python scripts/import_osm_infrastructure.py

# Calculate infrastructure proximity penalties
docker compose exec backend python scripts/update_hotspot_penalties.py

# Process SRTM elevation data (requires srtm_52_07.tif in backend/data/srtm/)
docker compose exec backend python scripts/calculate_elevation.py
docker compose exec backend python scripts/update_hotspot_elevation.py

# Seed 2024 monsoon historical data (122 days, real rainfall)
docker compose exec backend python scripts/seed_history.py
```

### 3. Open Dashboard

Navigate to **http://localhost:8000** — the full dashboard loads automatically.

### 4. Run a Cycle

Click **Run Cycle** on the control bar, or use the API:

```bash
curl -X POST http://localhost:8000/run/cycle \
  -H "X-API-Key: hydro-mvp-secret-2026"
```

### 5. Simulate a Cloudburst

Click **Cloudburst** on the dashboard, or manually:

```bash
# Push extreme rainfall across Delhi
curl -X POST http://localhost:8000/ingest/rain \
  -H "Content-Type: application/json" \
  -H "X-API-Key: hydro-mvp-secret-2026" \
  -d '{"geojson_polygon": {"type":"Polygon","coordinates":[[[77.0,28.5],[77.4,28.5],[77.4,28.8],[77.0,28.8],[77.0,28.5]]]}, "intensity_r": 8.5}'

# Then run scoring cycle
curl -X POST http://localhost:8000/run/cycle \
  -H "X-API-Key: hydro-mvp-secret-2026"
```

---

## Scoring Algorithm (PMRS)

The **Pump-dispatch Multi-factor Risk Score** combines four inputs per ward:

| Factor | Weight | Source |
|---|---|---|
| Rainfall intensity | 40% | Rain events overlapping ward hotspots |
| Drainage capacity | 30% | Hotspot capacity values (degraded by sensor events) |
| Infrastructure proximity | 20% | 7-tier penalty from nearby hospitals/substations/fire stations |
| Terrain elevation | 10% | SRTM-derived runoff multiplier (1.0x flat to 3.5x ridge) |

Wards scoring below threshold are flagged **TRIGGERED**. Wards with negative scores are **CRITICAL**. The LP solver then optimizes pump allocation across all triggered wards to minimize total unmet demand.

---

## Data Sources

| Data | Source | Details |
|---|---|---|
| Ward boundaries | [Datameet](https://github.com/datameet) | 290 Delhi wards, GeoJSON |
| Infrastructure | OpenStreetMap via Overpass API | 871 hospitals, 324 substations, 40 fire stations |
| Elevation | NASA SRTM 90m | Tile srtm_52_07 covering Delhi |
| 2024 Monsoon | [Open-Meteo](https://open-meteo.com/) | Jun 1-Sep 30 2024, daily totals, 922.9mm cumulative |
| Live weather | OpenWeatherMap 2.5 | Real-time rain detection + auto-cycle |
| Basemap tiles | CartoDB Positron | Light minimal basemap |

---

## Project Structure

```
urban-hydrology-engine/
├── docker-compose.yml              # 3-service stack
├── frontend/
│   └── index.html                  # Single-page dashboard (~1,800 lines)
└── backend/
    ├── Dockerfile                  # Python 3.11 + GDAL + g++
    ├── requirements.txt
    ├── app/
    │   ├── main.py                 # FastAPI app, static mount, startup
    │   ├── db.py                   # Async engine, ORM models
    │   ├── auth.py                 # X-API-Key verification
    │   ├── schemas.py              # Pydantic request/response models
    │   ├── api/
    │   │   ├── map_state.py        # All map, cycle, history routes
    │   │   └── ingest.py           # Rain + sensor ingest routes
    │   └── services/
    │       ├── scoring.py          # PMRS scoring algorithm
    │       ├── dispatch_lp.py      # PuLP LP solver
    │       ├── weather.py          # OWM polling + APScheduler
    │       └── geo.py              # Spatial utilities
    ├── data/
    │   ├── delhi_wards.geojson     # 290 ward boundaries
    │   ├── delhi_monsoon_2024.json # Real rainfall data
    │   ├── srtm/                   # SRTM elevation tiles
    │   └── osm/                    # OSM infrastructure JSONs
    └── scripts/
        ├── import_delhi_wards.py   # Ward boundary import
        ├── import_osm_infrastructure.py
        ├── update_hotspot_penalties.py
        ├── calculate_elevation.py
        ├── update_hotspot_elevation.py
        ├── seed_history.py         # 2024 monsoon simulation
        └── seed_city.py            # Original synthetic seed
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://hydro:hydro123@db:5432/hydrology` | PostGIS connection string |
| `OWM_API_KEY` | — | OpenWeatherMap API key |
| `API_SECRET_KEY` | — | X-API-Key header value for write endpoints |
| `CITY_LAT` | `28.65` | City center latitude |
| `CITY_LON` | `77.22` | City center longitude |
| `RAIN_POLL_INTERVAL_SECONDS` | `600` | Weather polling interval |
| `RAIN_COVERAGE_DEGREES` | `0.15` | Rainfall polygon radius in degrees |

---

## License

MIT
