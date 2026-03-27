# FloodReady Delhi — Comprehensive Project Audit
### For VP / Ministry Presentation — March 2026

---

## TABLE OF CONTENTS

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Backend — API Endpoints & Routes](#3-backend--api-endpoints--routes)
4. [Backend — Database Schema (6 Tables)](#4-backend--database-schema-6-tables)
5. [Backend — Core Algorithms & Scoring Engine](#5-backend--core-algorithms--scoring-engine)
6. [Backend — External Data Sources & Integrations](#6-backend--external-data-sources--integrations)
7. [Backend — Automated Seeding & Scripts](#7-backend--automated-seeding--scripts)
8. [Frontend — Complete Feature Map](#8-frontend--complete-feature-map)
9. [Frontend — Interactive Panels & Modals](#9-frontend--interactive-panels--modals)
10. [Frontend — Canvas Rendering Systems](#10-frontend--canvas-rendering-systems)
11. [Frontend — All JavaScript Functions (Index)](#11-frontend--all-javascript-functions-index)
12. [Data Sources — What's Real vs Simulated](#12-data-sources--whats-real-vs-simulated)
13. [Deployment & Infrastructure](#13-deployment--infrastructure)
14. [Dependencies & Libraries](#14-dependencies--libraries)
15. [Key Constants & Thresholds (Reference Table)](#15-key-constants--thresholds-reference-table)
16. [Known Limitations](#16-known-limitations)

---

## 1. Executive Summary

**FloodReady Delhi** is a Pre-Monsoon Intelligence System that monitors, scores, and dispatches flood response resources across all **290 administrative wards** of Delhi NCT (MCD + NDMC + Cantonment Board).

### What It Does (One Paragraph)
The system ingests live rainfall data (IMD, OpenWeatherMap, Open-Meteo), monitors the Yamuna river via CWC Hathnikund gauge data, and runs a proprietary **three-mechanism PMRS v3 scoring engine** (Pluvial 45% + Fluvial 30% + Compound 25%) on each ward every 10 minutes. When a ward's score drops below 65, it is flagged as "triggered" and the system auto-dispatches mobile pumps via an integer linear program (LP) solver. The system also provides pre-monsoon readiness scoring, a 2023 flood replay backtest with precision/recall metrics, 6-hour rainfall forecasts, a System Saturation Index (SSI), Yamuna flood zone mapping, drain sensor monitoring, CCTV drain cameras, QRT vehicle tracking, and exportable HTML reports for municipal officers.

### Key Numbers
| Metric | Value |
|--------|-------|
| Wards monitored | 290 (full Delhi NCT) |
| Hotspot points | ~32,000 (generated per ward area) |
| Critical infrastructure | ~2,900 (hospitals, substations, fire stations) |
| Historical dispatch records | 35,380 (2024 monsoon: 122 days × 290 wards) |
| Scoring cycle time | < 3 seconds (3 SQL queries for all 290 wards) |
| Backtest events | 6 (Jul 9–14, 2023 sequence) |
| External APIs | 5 (IMD, OpenWeatherMap, Open-Meteo, CWC, Open-Meteo Elevation) |

### Technology Stack
- **Backend**: Python 3.11, FastAPI 0.111.0, SQLAlchemy 2.0 (async), PostgreSQL + PostGIS
- **Frontend**: Single-file HTML/CSS/JS SPA (8,128 lines), Leaflet.js, Chart.js, Canvas API
- **Solver**: PuLP (CBC integer LP) for pump dispatch optimization
- **Deployment**: Render (Docker), auto-seed on first deploy, APScheduler for background jobs
- **Real-time**: WebSocket broadcast for cycle results + SSI updates

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        BROWSER (SPA)                             │
│  index.html — 8,128 lines                                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────┐   │
│  │ Leaflet  │ │ Chart.js │ │ Canvas   │ │ WebSocket Client │   │
│  │ Map +    │ │ History  │ │ Radar +  │ │ Real-time cycle  │   │
│  │ GeoJSON  │ │ + Drill  │ │ CCTV     │ │ updates          │   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────────────┘   │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP + WebSocket
┌────────────────────────────┴────────────────────────────────────┐
│                    FastAPI Backend (:8000)                        │
│                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐  │
│  │ API Routes      │  │ Services        │  │ Background     │  │
│  │ • /run/cycle    │  │ • scoring.py    │  │ • APScheduler  │  │
│  │ • /map/state    │  │ • readiness.py  │  │   weather poll │  │
│  │ • /ingest/rain  │  │ • dispatch_lp.py│  │   Hathnikund   │  │
│  │ • /readiness/*  │  │ • forecast.py   │  │   self-ping    │  │
│  │ • /backtest/*   │  │ • weather.py    │  │ • Auto-seed    │  │
│  │ • /yamuna/*     │  │ • yamuna.py     │  │   (first run)  │  │
│  │ • /export/*     │  │ • ssi.py        │  │                │  │
│  │ • /ws (WS)      │  │ • geo.py        │  │                │  │
│  └─────────────────┘  └─────────────────┘  └────────────────┘  │
│                             │                                    │
│                    ┌────────┴────────┐                           │
│                    │ PostgreSQL +    │                           │
│                    │ PostGIS         │                           │
│                    │ 6 tables        │                           │
│                    │ GIST indexes    │                           │
│                    └─────────────────┘                           │
└──────────────────────────────────────────────────────────────────┘
         │                    │                    │
    ┌────┴────┐         ┌────┴────┐         ┌────┴────┐
    │ IMD     │         │ CWC     │         │ Open-   │
    │ Delhi   │         │ India-  │         │ Meteo   │
    │ Weather │         │ WRIS    │         │ Forecast│
    └─────────┘         └─────────┘         └─────────┘
```

### File Structure (Key Files Only)

```
urban-hydrology-engine/
├── backend/
│   ├── app/
│   │   ├── main.py          ← FastAPI app, startup, routes, CORS, scheduler
│   │   ├── db.py            ← SQLAlchemy models (6 tables), engine config
│   │   ├── schemas.py       ← Pydantic request/response models
│   │   ├── auth.py          ← X-API-Key header verification
│   │   ├── auto_seed.py     ← First-deploy auto-population
│   │   ├── ws.py            ← WebSocket connection manager
│   │   ├── api/
│   │   │   ├── ingest.py    ← POST /ingest/rain, /ingest/sensor, /ingest/degrade-polygon
│   │   │   ├── map_state.py ← GET /map/state, /map/hotspots, POST /run/cycle, /reset
│   │   │   ├── readiness.py ← GET /readiness/scores, /readiness/summary
│   │   │   ├── ward_detail.py ← GET /ward/{id}/detail
│   │   │   ├── backtest.py  ← POST /backtest/2023, GET /backtest/2023/status
│   │   │   ├── floodline.py ← GET /map/floodline
│   │   │   └── export.py    ← GET /export/ward/{id}, /export/summary
│   │   └── services/
│   │       ├── scoring.py   ← PMRS v3 three-mechanism engine
│   │       ├── readiness.py ← Static pre-monsoon readiness scores
│   │       ├── dispatch_lp.py ← PuLP integer LP pump allocation
│   │       ├── forecast.py  ← Open-Meteo 6-hour forecast
│   │       ├── weather.py   ← IMD + OWM + Open-Meteo rainfall
│   │       ├── yamuna.py    ← CWC Hathnikund scraper
│   │       ├── cwc_scraper.py ← CWC data extraction
│   │       └── ssi.py       ← System Saturation Index
│   ├── data/
│   │   ├── delhi_wards.geojson         ← 290 ward boundaries (Datameet)
│   │   ├── delhi_ward_elevation.json   ← Pre-computed SRTM elevation per ward
│   │   ├── delhi_monsoon_2024.json     ← Open-Meteo historical rainfall
│   │   └── osm/                        ← Overpass API exports
│   └── scripts/
│       ├── import_delhi_wards.py       ← Seed 290 wards + ~32k hotspots
│       ├── import_osm_infrastructure.py ← Seed hospitals/substations/fire stations
│       ├── seed_elevation_static.py    ← Populate ward_elevation table
│       ├── seed_history.py             ← Generate 2024 monsoon dispatch history
│       ├── update_hotspot_penalties.py ← OSM proximity → penalty scores
│       └── calculate_elevation.py      ← SRTM raster → ward statistics
├── frontend/
│   └── index.html   ← Entire frontend (8,128 lines: HTML + CSS + JS)
├── Dockerfile.render
├── docker-compose.yml
└── render.yaml
```

---

## 3. Backend — API Endpoints & Routes

### Core Operations

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/` | — | Serve frontend/index.html |
| `GET` | `/ping` | — | Lightweight keep-alive (no DB) |
| `GET` | `/health` | — | DB status, table counts, metadata |
| `POST` | `/run/cycle` | API Key | Score all 290 wards (3 queries), dispatch pumps, broadcast via WS |
| `POST` | `/reset` | API Key | Clear rain/sensor/dispatch data, restore baseline capacity |
| `GET` | `/map/state` | — | GeoJSON FeatureCollection of 290 wards with latest dispatch scores |
| `GET` | `/map/hotspots` | — | Hotspot centroids (bbox-filtered, max 8000, from memory cache) |
| `GET` | `/city/bounds` | — | Bounding box + center + rain polygon for city |

### Rain & Sensor Ingestion

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `POST` | `/ingest/rain` | API Key | Store rainfall polygon + intensity, count intersecting hotspots |
| `POST` | `/ingest/sensor` | — | Store sensor delta, update hotspot capacity, invalidate SSI cache |
| `POST` | `/ingest/degrade-polygon` | API Key | Batch-reduce capacity for all hotspots in a polygon (cloudburst sim) |

### Intelligence

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/readiness/scores` | — | All 290 wards with static PMRS readiness + flood type + mechanisms |
| `GET` | `/readiness/summary` | — | Aggregate stats (red/amber/green counts, avg score, days to monsoon) |
| `GET` | `/readiness/ward/{id}` | — | Single ward readiness detail |
| `GET` | `/ward/{id}/detail` | — | Historical 30-day analysis + infrastructure + terrain |
| `GET` | `/forecast/6hour` | — | Open-Meteo hourly precipitation, risk level, predicted triggered wards |
| `GET` | `/weather/status` | — | Cached live weather (IMD/OWM/Open-Meteo with source attribution) |
| `GET` | `/yamuna/status` | — | CWC Hathnikund discharge, predicted Delhi stage, ETA, alert level |
| `GET` | `/ssi` | — | System Saturation Index (composite 0–100 stress metric) |

### Backtest

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `POST` | `/backtest/2023` | API Key | Enqueue 6-event 2023 replay as background task |
| `GET` | `/backtest/2023/status` | — | Poll job progress (queued/running/complete/error) |
| `GET` | `/backtest/2023/result` | — | Full result with precision/recall/F1 + per-ward classification |

### Flood Zones & Export

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET` | `/map/floodline` | — | Yamuna flood zone classification (Zone 1–4 by elevation) |
| `GET` | `/export/ward/{id}` | — | Printable HTML5 ward card (hero, metrics, sparkline, actions) |
| `GET` | `/export/summary` | — | A4 printable league table — all 290 wards ranked |

### WebSocket

| Endpoint | Direction | Messages |
|----------|-----------|----------|
| `WS /ws` | Server→Client | `cycle` (wards_triggered, safe_wards), `ssi_update`, `backtest_progress`, `backtest_complete`, `backtest_error` |

### Startup Sequence (on boot)

```
1. init_db() — Create PostGIS extension + all 7 tables (30 retries, 3s apart)
2. Load hotspot centroids into memory cache
3. Auto-seed check: if wards table empty → background thread runs 5-step seeding
4. Seed elevation data (background thread)
5. Start APScheduler:
   • weather_poll_job   — every 600s (10 min)
   • hathnikund_scrape  — every 3600s (1 hour)
   • self_ping          — every 780s (13 min, for Render keep-alive)
6. Initial weather fetch + Hathnikund scrape
```

---

## 4. Backend — Database Schema (6 Tables)

### Table: `wards` (290 rows)
| Column | Type | Description |
|--------|------|-------------|
| id | Integer PK | Auto-increment |
| name | String | Ward name (e.g. "Shahdara") |
| ward_no | String (nullable) | MCD ward number |
| zone_name | String (nullable) | Delhi zone (New Delhi, South, North, East, West, Central, Outer) |
| geom | Geometry(GEOMETRY, 4326) | Ward polygon boundary |
| **Index** | `idx_wards_geom` (GIST) | Spatial queries |

### Table: `hotspots` (~32,000 rows)
| Column | Type | Description |
|--------|------|-------------|
| id | Integer PK | Auto-increment |
| ward_id | Integer FK→wards | Parent ward |
| geom | Geometry(POLYGON, 4326) | Small area (~2km²) |
| capacity_c | Float (default 100) | Current drainage capacity 0–100 (degrades during events) |
| baseline_capacity_c | Float | Pre-monsoon baseline (zone-specific: East Delhi 35-65, New Delhi 70-95) |
| runoff_t | Float (default 1.0) | Terrain runoff multiplier 1.0–3.5 (higher = worse drainage) |
| priority_weight | Float (default 1.0) | LP dispatch priority (OSM proximity-boosted) |
| critical_penalty_pc | Float (default 0) | Penalty from nearby critical infrastructure (0–200) |
| zone_name | String (nullable) | Inherited from ward |

### Table: `rain_events`
| Column | Type | Description |
|--------|------|-------------|
| id | Integer PK | Auto-increment |
| geom | Geometry(POLYGON, 4326) | Rainfall coverage polygon |
| intensity_r | Float | Rainfall intensity (mm/hr) |
| created_at | DateTime | Timestamp |

### Table: `sensor_events`
| Column | Type | Description |
|--------|------|-------------|
| id | Integer PK | Auto-increment |
| hotspot_id | Integer FK→hotspots | Target hotspot |
| delta_capacity | Float | Change to capacity (can be negative) |
| created_at | DateTime | Timestamp |

### Table: `dispatch_runs` (~35,380 seeded + live)
| Column | Type | Description |
|--------|------|-------------|
| id | Integer PK | Auto-increment |
| ward_id | Integer FK→wards | Scored ward |
| ws_score | Float | PMRS v3 composite score |
| status | String | 'safe' / 'triggered' / 'dispatched' / 'critical' |
| result_json | JSON | Pump assignments, dispatch message |
| created_at | DateTime | Timestamp |

### Table: `critical_infrastructure` (~2,900 rows)
| Column | Type | Description |
|--------|------|-------------|
| id | Integer PK | Auto-increment |
| osm_id | BigInteger | OpenStreetMap ID |
| facility_type | String | 'hospital' / 'substation' / 'fire_station' |
| name | String (nullable) | Facility name |
| lat, lon | Float | Coordinates |
| geom | Geometry(POINT, 4326) | Spatial point |

### Table: `ward_elevation` (290 rows)
| Column | Type | Description |
|--------|------|-------------|
| ward_id | Integer PK FK→wards | Ward reference |
| mean_elevation | Float | Metres above MSL |
| min_elevation, max_elevation | Float | Range |
| elevation_range | Float | Max - min |
| mean_slope, max_slope | Float | Degrees |
| runoff_t | Float | Horton-derived multiplier |
| terrain_class | String | 'floodplain' / 'flat' / 'moderate' / 'steep' / 'ridge' |

---

## 5. Backend — Core Algorithms & Scoring Engine

### 5.1 PMRS v3 — Three-Mechanism Real-Time Scoring

The core scoring engine computes a 0–100 score for each ward based on three independent flood mechanisms:

```
PMRS = 0.45 × Pluvial + 0.30 × Fluvial + 0.25 × Compound
```

#### Mechanism 1: Pluvial (45%) — Rain-on-drain flooding

```
For each hotspot in the rain polygon (3-hour window):
  1. Horton soil saturation: sat_factor = max(0.80, 1.0 - (event_count - 1) × 0.05)
  2. Raw score = (capacity_c × sat_factor) / (peak_intensity × runoff_t)
  3. Clamped = min(100, raw × PLUVIAL_SCALE=18.5)

Aggregate across all hotspots:
  pluvial = mean(all_scores) - mean(critical_penalty_pc)/20 + TERRAIN_ADJ[terrain_class]
```

**Terrain adjustments**: floodplain −15, low −8, flat 0, moderate +3, steep +6, ridge +8

**Key constants**:
- `PLUVIAL_SCALE = 18.5` — normalizes C/(R×T) to 0–100
- `RAIN_WINDOW_MIN = 180` — 3-hour accumulation window
- `HORTON_F_INF = 0.635 mm/hr` — equilibrium infiltration (Group D loam)
- `HORTON_K_DECAY = 0.00115 s⁻¹` — decay constant

#### Mechanism 2: Fluvial (30%) — River & outfall submergence

```
Yamuna penalty (elevation-weighted):
  If elevation ≥ 215m → 0 (no effect)
  If elevation ≤ 207m → full YAMUNA_PENALTY[status]
  Else → linear taper: penalty × (215 - elevation) / 8

Terrain vulnerability:
  Floodplain → −20, Low → −10, Others → 0

fluvial = 100 - elevation_penalty - terrain_vulnerability
```

**Yamuna status penalties**: NORMAL=0, WATCH=8, WARNING=20, DANGER=35, EXTREME=50

#### Mechanism 3: Compound (25%) — Simultaneous rain + high river

```
drain_surcharge = pluvial_stress × rain_norm × (0.3 + 0.7 × elev_vuln) × 60
backflow = pluvial_stress × river_norm × elev_vuln × 60
compound = 100 - drain_surcharge - backflow
```

This component only activates severely when BOTH rain AND river are elevated AND the ward is low-lying (multiplicative interaction).

#### Status Thresholds

| Score | Status | Color | Action |
|-------|--------|-------|--------|
| ≥ 75 | Safe | Green | Monitor |
| 65–75 | At risk | Yellow | Watch |
| 40–65 | Triggered | Orange/Amber | Dispatch pumps |
| < 40 | Critical | Red | Emergency response |

#### Batch Optimization

Instead of 870+ per-ward queries, the system uses **3 SQL queries total**:
1. All 290 wards with elevation data
2. Hotspot counts per ward
3. Rain-affected hotspots with peak intensity (spatial join `ST_Intersects`)

### 5.2 Static Readiness Scoring (Pre-Monsoon)

Same three-mechanism model but computed from **historical patterns** (no live rain):

```
Pluvial readiness: 100 - 40×flood_risk_norm - 30×(1-capacity_norm) - 10×infra_norm
Fluvial readiness: 100 - 50×elev_risk - 30×terrain_penalty
Compound readiness: 100 - (pluvial_vuln × fluvial_vuln × 70) - (runoff_amp × 15)

Readiness = 0.45×pluvial_r + 0.30×fluvial_r + 0.25×compound_r
```

**Bands**: ≥65 Green (Ready), 40–65 Amber (At Risk), <40 Red (Critical)

**Flood type classification**:
- **River**: elevation <215m or terrain='floodplain' or Yamuna-adjacent name
- **Backflow**: river ward + high runoff_t ≥2.8 or trigger_rate >0.1
- **Sustained**: non-floodplain + runoff_t ≥3.0 or trigger_rate >0.15
- **Pluvial**: all others (most common)

### 5.3 LP Pump Dispatch (PuLP)

```
Maximize: Σ (priority_weight[i] × x[i])   for each hotspot i

Subject to:
  Σ x[i] ≤ 15                              (max pumps per ward)
  x[i] ≤ 3                                 (max per hotspot)
  x[i] ≥ 1  if critical_penalty_pc > 0     (critical infra guaranteed)
  x[i] ∈ {0, 1, 2, 3}                      (integer)

Solver: CBC (COIN-OR Branch and Cut)
Input: Top 30 hotspots by priority_weight
Output: {ward_id, pumps_total, assignments: [{hotspot_id, pumps, priority}]}
```

### 5.4 System Saturation Index (SSI)

```
SSI = 100 × (0.40×rain_5d + 0.30×trigger_5d + 0.20×critical_5d + 0.10×capacity_stress)

Where:
  rain_5d = min(1.0, total_5d_intensity / 50.0)
  trigger_5d = triggered_wards / total_wards
  critical_5d = critical_wards / total_wards
  capacity_stress = (100 - mean_capacity) / 100
```

**Levels**: <30 Normal (green), 30–50 Elevated (yellow), 50–75 Watch (amber), ≥75 Pre-position (red)

### 5.5 6-Hour Rainfall Forecast

```
Source: Open-Meteo (free, no API key)
Endpoint: api.open-meteo.com/v1/forecast (Delhi 28.65°N, 77.22°E)

Risk levels:
  ≥ 50mm → EXTREME
  ≥ 25mm → HIGH
  ≥ 10mm → MODERATE
  ≥ 2mm  → LOW
  < 2mm  → NONE

Cache: 10 minutes
```

### 5.6 Yamuna Advance Warning

```
Source priority: CWC FloodWatch → CWC India-WRIS → Open-Meteo upstream proxy

Thresholds (Old Railway Bridge, Delhi):
  Normal    <202.0m
  Warning   204.22m (validated: INDOFLOODS gauge-164, 48 yrs)
  Danger    205.33m (validated: INDOFLOODS gauge-164)
  High flood 206.0m
  Extreme   208.0m  (2023 peak: 208.66m)

Travel time Hathnikund → Delhi:
  ≥500k cusecs → 48h
  ≥300k cusecs → 52h
  ≥200k cusecs → 56h
  ≥100k cusecs → 60h
  <100k cusecs → 72h

Cache: 15 minutes
```

### 5.7 Backtest Engine (July 2023 Replay)

6-event synthetic sequence (Jul 9–14, 2023):
- Days 9–11: City-wide polygon (pluvial)
- Days 12–14: Yamuna corridor polygon (fluvial overbank)

**Reference classification** (what actually flooded):
- Elevation <207m: flooded
- East Delhi <210m: flooded
- North corridor names <210m: flooded
- Central backflow <212m: flooded

**Outputs**: Precision, Recall, F1 Score, per-ward classification (TP/FP/FN/TN)

---

## 6. Backend — External Data Sources & Integrations

### 6.1 Live Weather (3 sources, priority cascade)

| Priority | Source | Method | What It Provides |
|----------|--------|--------|------------------|
| 1 | IMD Delhi | HTML scraping (regex) | 24h rainfall, current conditions |
| 2 | OpenWeatherMap | REST API (`OWM_API_KEY`) | rain.1h, temperature, conditions |
| 3 | Open-Meteo | REST API (free) | Current precipitation, WMO weather code |

All sources generate a ±0.15° polygon around coordinates and intensity_r in mm/hr.

### 6.2 Yamuna / Hathnikund (3 sources, priority cascade)

| Priority | Source | Endpoint | Data |
|----------|--------|----------|------|
| 1 | CWC FloodWatch | ffs.india-water.gov.in | Live discharge, forecast |
| 2 | CWC India-WRIS | indiawris.gov.in | Real-time gauge data |
| 3 | Open-Meteo Upstream | Haridwar (29.95°N, 78.16°E) | Proxy: 24h rain × 5,000 cusecs/mm |

### 6.3 Static Data Files

| File | Source | Records | Purpose |
|------|--------|---------|---------|
| delhi_wards.geojson | Datameet OSM + MCD | 290 polygons | Ward boundaries |
| delhi_ward_elevation.json | Open-Meteo Elevation API | 290 rows | Pre-computed SRTM statistics |
| delhi_monsoon_2024.json | Open-Meteo Historical | 122 days | Monsoon rainfall sequence |
| osm/hospitals.json | Overpass API | ~1,500 | Hospital locations |
| osm/substations.json | Overpass API | ~1,000 | Power substations |
| osm/fire_stations.json | Overpass API | ~400 | Fire station locations |

---

## 7. Backend — Automated Seeding & Scripts

### Auto-Seed Sequence (first deploy)

On startup, if the `wards` table is empty, a background thread runs:

| Step | Script | What It Does | Output |
|------|--------|-------------|--------|
| 1 | `import_delhi_wards.py` | Load 290 ward polygons + generate hotspots via `ST_GeneratePoints()` | 290 wards, ~32k hotspots |
| 2 | `import_osm_infrastructure.py` | Load hospital/substation/fire station locations | ~2,900 facilities |
| 3 | `update_hotspot_penalties.py` | Calculate OSM proximity penalties (100m/250m/500m tiers) | Updated priority_weight + critical_penalty_pc |
| 4 | `seed_elevation_static.py` | Populate ward_elevation from pre-computed JSON | 290 elevation records |
| 5 | `seed_history.py` | Generate 2024 monsoon dispatch history (Jun 1–Sep 30) | 35,380 dispatch_run rows |

### Hotspot Generation Logic

```
Density: 1 hotspot per 15,000 m² of ward area (MIN 5, MAX 150 per ward)

Capacity by zone:
  New Delhi:    70–95 (best maintained drains)
  South Delhi:  55–85
  Central Delhi: 50–80
  North Delhi:  40–70
  West Delhi:   45–75
  East Delhi:   35–65 (Trans-Yamuna alluvium, poorest drainage)
  Outer Delhi:  30–60 (least developed)

Runoff multiplier by zone:
  East Delhi:   2.5–3.5 (floodplain, alluvial clay)
  South Delhi:  1.0–1.8 (ridge, good gradient)
  Others:       1.5–2.8 (mixed urban)
```

### OSM Proximity Penalties

| Tier | Distance | Hospital | Substation | Fire Station |
|------|----------|----------|------------|--------------|
| 1 | 100m | penalty=200, weight=2.0 | penalty=150, weight=1.8 | penalty=100, weight=1.5 |
| 2 | 250m | penalty=100, weight=1.6 | penalty=75, weight=1.4 | penalty=50, weight=1.3 |
| 3 | 500m | penalty=25, weight=1.2 | penalty=25, weight=1.2 | penalty=25, weight=1.2 |

---

## 8. Frontend — Complete Feature Map

The entire frontend is a single HTML file (8,128 lines) with no build tools. Libraries loaded via CDN:
- **Leaflet 1.9.4** — Interactive map + GeoJSON rendering
- **Leaflet.VectorGrid 1.3.0** — Vector tile support
- **Leaflet.heat 0.2.0** — Heatmap overlay
- **Chart.js 4.4.0** — History + drill-down charts
- **chartjs-plugin-annotation 3.0.1** — Chart threshold lines

### 8.1 Header Bar (60px navy)

| Element | ID | Description |
|---------|----|-------------|
| Brand | `.brand` | FloodReady Delhi logo + subtitle "Municipal Hydrology Command · Real-Time" |
| Triggered pills | `#m-triggered` | Live count of triggered wards |
| Pumps pill | `#m-pumps` | Total dispatched pumps |
| Weather pill | `#pill-weather` | Live weather with condition icon (rain/sun/cloud) |
| Monsoon countdown | `#countdown-widget` | Days:Hours:Minutes until June 15 monsoon onset |
| Ward search | `#ward-search` | Autocomplete search across 290 wards |
| Present mode | `#present-btn` | Toggle presentation mode (hides controls, expands map) |
| Methodology | `#btn-methodology` | Opens PMRS methodology modal with citations |
| Theme toggle | `#btn-theme` | Dark/light mode (persisted to localStorage) |
| Connection status | `#conn-status` | Green/yellow/red dot + "Live"/"Reconnecting…" |
| Clock | `#header-clock` | IST time, updated every 30s |
| Alert strip | `#header-alert-strip` | 3px colored bar (green/yellow/red/pulsing) |

### 8.2 Alert Banner (36px, collapsible)

| Element | ID | Description |
|---------|----|-------------|
| Status | `#ab-status` | "System Normal" / "⛈ Cloudburst…" |
| Triggered count | `#ab-triggered` | "X wards triggered" |
| SSI | `#ab-ssi` | "SSI 42 · Watch" |
| Yamuna | `#ab-yamuna` | "Yamuna: Normal" |

Colors: green (normal), yellow (elevated), red (danger), dark red (critical)

### 8.3 Map (Leaflet)

| Layer | Source | Description |
|-------|--------|-------------|
| Base tiles | CARTO Light/Dark | Toggle with theme |
| Ward choropleth | `/map/state` GeoJSON | 290 polygons colored by PMRS score |
| Hotspot centroids | `/map/hotspots` | Canvas-rendered dots (red→green by capacity_c) |
| Elevation overlay | `/map/floodline` | SRTM-based 5-color elevation choropleth |
| Infrastructure markers | In-memory JSON | Hospital (red), Substation (amber), Fire station (blue) dots |
| QRT vehicle markers | Client-side | 8 animated 🚒 markers at PWD depot locations |
| Flood zone lines | `/map/floodline` | Yamuna Zone 1–4 classification |
| Flood type highlights | Client-side filter | River (blue), Pluvial (amber), Backflow (purple), Sustained (red) |
| Backtest overlay | Client-side | TP (green), FN (red), FP (amber) ward coloring |

**Map controls**: Floating toggle switches for elevation layer, infrastructure markers, flood zone overlays.

### 8.4 Ward Popups (on click)

Each ward polygon click opens a rich popup containing:
1. **Header**: Ward name + zone
2. **Readiness score**: Large number with color + band label
3. **Uncertainty note**: ±15% proxy uncertainty
4. **Three-mechanism breakdown**: Pluvial / Fluvial / Compound sub-scores
5. **Top 2 risk factors**: Flood Risk, Drain Deficit, Elevation, Infra Exposure
6. **Recommended action**: Band-specific (deploy pumps / pre-position / monitor)
7. **Flood probability**: Computed from score + elevation + basin (with progress bar)
8. **Estimated inundation depth**: Min–max metres above road level
9. **Data source badges**: REAL (SRTM) + PROXY (index model) labels
10. **Dispatch status**: Safe/Triggered/Critical + dispatch message
11. **Terrain info**: Terrain class, slope multiplier, elevation
12. **View Details button**: Opens drill-down panel
13. **SMS Alert button**: (Red/Amber wards only) Generate resident alert text

### 8.5 Control Bar (56px)

| Button | ID | Action |
|--------|----|--------|
| Run Cycle | `#btn-cycle` | Primary cyan — triggers `/run/cycle` |
| Reset | `#btn-reset` | Ghost — clears all live data |
| Cloudburst | `#btn-storm` | Red danger — opens scenario picker modal |
| 2023 Replay | `#btn-backtest` | Ghost red with pulsing dot — opens backtest panel |
| Readiness | `#btn-readiness` | Ghost cyan — opens readiness panel |
| Radar | `#btn-radar` | Ghost green — opens radar simulation panel |

### 8.6 Side Panel (380px, right side)

Four tabs: **Overview** | **Risk** | **Sensors** | **CCTV**

#### Overview Tab
- **Stat cards** (2×2 grid): Last Cycle time, Triggered count, Pumps Active, Wards Online
- **Yamuna/Hathnikund strip**: Discharge (cusecs), Predicted Delhi stage (m), ETA (hours), source badge (Live/Proxy), data source badges
- **6-Hour Forecast**: Risk level pill, hourly precipitation bars, estimated triggered wards

#### Risk Tab
- **Summary grid**: Critical (red), At Risk (amber), Ready (green) counts
- **SSI bar**: Progress bar with value + level label
- **Ward search**: Filter by name
- **Sorted ward list**: All 290 wards by score (worst first) with click-to-drill

#### Sensors Tab
- **18 drain sensors** (Delhi's major drains with real names and locations)
- Per sensor: Name, location, basin, water level (m), trend arrow, status badge, capacity bar
- Status counts: Normal / Warning / Critical / Blocked
- Data source badges: REAL drain names & CWC thresholds, SIMULATED live readings

#### CCTV Tab
- **6 drain cameras** (real drain locations, simulated feeds)
- Per camera: Canvas-animated feed (water, rain, debris, scanlines), status badge, level bar
- Status counts: Normal / Alert / Offline
- Data source badges: REAL drain locations, SIMULATED camera feeds

---

## 9. Frontend — Interactive Panels & Modals

### 9.1 Readiness Panel (420px, slides from left)

- Monsoon countdown (days until June 15)
- Band summary: Red / Amber / Green counts
- SSI gauge with level labels
- Flood type filter pills: All / River / Pluvial / Backflow / Sustained
- Search bar
- Quick-count bar (red/amber/green)
- Scrollable 290-ward list with rank, name, zone, score, flood type dot
- Export button → `/export/summary`

### 9.2 Ward Drill-Down Panel (400px, slides from left)

- Ward name, zone, terrain class
- Status badge (Safe/Triggered/Critical)
- Readiness score bar with fill + flood type badge + ±15% uncertainty
- Three-mechanism sub-score cards (Pluvial/Fluvial/Compound with weights)
- 2023 backtest match row (TP/FP/FN/TN)
- Key metrics grid: Elevation, Runoff T, Hotspots, Critical hotspots
- Infrastructure within 500m: Hospitals, Substations, Fire stations
- 30-day score history chart (Chart.js line with threshold annotations)
- Summary narrative
- Export PDF button → `/export/ward/{id}`

### 9.3 Backtest Panel (460px, slides from right)

**Pre-run state**:
- Narrative strip: "208.66 metres" hero stat, 153mm rainfall, 27,000+ families, ₹1,200Cr
- 6-event horizontal timeline (Jul 9–14)
- "Run Replay" button

**Post-run state**:
- Precision / Recall / F1 Score metric cards
- PR statements (e.g., "92.5% of wards that flooded were flagged by our model")
- Confusion matrix (TP/FP/FN/TN with counts + descriptions)
- Event timeline (6 days with rain bars + ward counts)
- Sample comparison table (5 wards with prediction vs reality)
- Verdict block with key stats
- Ward results list with filter pills (All/TP/FP/FN/TN)

### 9.4 History Panel (slides up, 52vh)

- Season selector (2024 monsoon / Last 30 days)
- Data source badge (e.g., "Real rainfall data — Open-Meteo / Delhi 2024")
- Chart.js area chart: triggered + critical wards per day
- Sidebar: Peak day, Avg daily triggered, Total dispatches, Most affected zone
- Worst wards table (rank, name, triggers, critical days, avg score)

### 9.5 Cloudburst Scenario Modal

Four scenarios based on real Delhi events:
1. **Sep 12 2024 — SW Delhi** (100.9mm, Extreme)
2. **Jul 9 2023 — Rohini / North Delhi** (153mm, Extreme)
3. **Aug 2023 — Yamuna Corridor / East Delhi** (78mm, Heavy)
4. **Sep 2021 — Central / West Delhi** (65mm, Moderate)

Each has: polygon, intensity_r (derived from total ÷ 3hr peak), delta_capacity, real location description.

### 9.6 Methodology Modal

6 sections:
1. Three-Mechanism Scoring Model (formula + IIT Delhi DMP basis)
2. Pluvial mechanism (C_eff / R_peak × T_runoff × 18.5)
3. Fluvial mechanism (elevation + Yamuna penalty + terrain)
4. Compound mechanism (multiplicative rain × river × elevation)
5. Data Sources (8 sources listed)
6. Research Citations (6 citation cards: IIT Delhi DMP, SWMM, CWC, INDOFLOODS, DDMA, Basin delineation)
7. Coverage (290 wards)
8. Validation (2023 backtest + INDOFLOODS gauge validation)

### 9.7 Data Sources Modal

Full transparency table showing every data source with:
- Real / Simulated / Proxy badge
- Source attribution
- Uncertainty range

### 9.8 SMS Alert Modal

For red/amber wards, generates a resident alert SMS template with:
- Ward name, flood probability, expected depth, recommended actions
- DDMA helpline numbers
- Formatted for copy-to-clipboard

---

## 10. Frontend — Canvas Rendering Systems

### 10.1 IMD Palam DWR Radar Simulation

Realistic radar rendering using value noise + fractal Brownian motion:

**Key functions**:
- `_valNoise(x, y)` — value noise hash
- `_fbm(x, y, oct)` — fractal Brownian motion (4–6 octaves)
- `_dBZtoRGBA(dBZ, alpha)` — 13-step IMD standard reflectivity color table (5–65 dBZ)
- `_mmhrToDBZ(mmhr)` — Marshall-Palmer Z=200R^1.6 conversion
- `_intensityToColor(intensity, alpha)` — mm/hr to radar color

**Rendering pipeline**:
1. `_renderRadarBase(W, H, frame)` — **Expensive offscreen canvas** (cached):
   - Pixelated 4px grid bins for radar texture
   - Noise-deformed storm shapes from hotspot/rain data
   - Range rings at 50km/100km/150km/200km
   - Compass rose (N/S/E/W)
   - Lat/lon grid with labels
   - NCT Delhi boundary polygon
   - Yamuna river trace
   - Ground clutter noise
   - Location crosshair markers
   - Cached in `_radarBaseCanvas` — only recomputes on frame/scenario change

2. `drawRadarFrame(frame)` — **Lightweight overlay** (every rAF):
   - Blits cached base canvas
   - Animated sweep line (cyan) with afterglow trail
   - IMD header text + timestamp
   - Max dBZ readout

3. `_radarLoop()` — Continuous `requestAnimationFrame` loop

**Radar site**: IMD Palam DWR at 28.585°N, 77.088°E

### 10.2 CCTV Drain Camera Feeds

6 canvas-animated camera feeds with per-frame rendering:

**`renderCameraCanvas(canvasId, status, camIndex)`**:
- Road surface (perspective trapezoid)
- Drain channel
- Water surface with sinusoidal waves (amplitude scales with severity)
- Floating debris (leaves, plastic — physics simulation)
- Rain drops (speed/count scale with intensity)
- Rain splash effects
- Scanline overlay (CRT effect)
- Noise grain (severe states)
- Timestamp bar (monospace with IST)
- Right-side level bar (colored gauge)
- Alert flash (red tint on critical/blocked)
- Day/night variation (alternating cameras)

**States**: normal (8% water), warning (35%), critical (62%), blocked (82%)

---

## 11. Frontend — All JavaScript Functions (Index)

### Configuration & Constants
| Variable | Line | Description |
|----------|------|-------------|
| `API_KEY` | 3553 | 'hydro-mvp-secret-2026' |
| `DELHI_SENSORS[18]` | 3560 | Drain sensor definitions (name, lat/lon, basin, thresholds) |
| `QRT_DEPOTS[8]` | 3588 | PWD Depot locations for QRT vehicles |
| `CCTV_CAMERAS[6]` | 3604 | CCTV camera definitions linked to drain sensors |
| `CLOUDBURST_SCENARIOS[4]` | 5098 | Four historical cloudburst events with polygons |
| `_DBZ_COLORS[13]` | ~7490 | IMD dBZ reflectivity color table |
| `_RADAR_SITE` | ~7570 | IMD Palam DWR coordinates |

### Sensor System (~180 lines)
- `initSensors()` — Initialize 18 sensor levels
- `getSensorStatus()` / `getSensorColor()` — Status classification
- `renderSensorList()` — Render all 18 sensor cards
- `updateSensorStats()` — Aggregate status counts
- `tickSensors()` — Simulate level changes (random walk)
- `spikeAllSensors(intensity)` — Cloudburst response (basin-weighted)

### QRT Vehicle Tracker (~70 lines)
- `initQRTMarkers()` — Place 8 🚒 markers at PWD depots
- `dispatchQRTToWards(dispatches)` — Animate vehicles to triggered wards (ease-in-out)
- `resetQRTMarkers()` — Return all vehicles to depots

### CCTV System (~250 lines)
- `initCCTV()` — Initialize 6 camera states
- `tickCCTV()` — Simulate water level changes
- `spikeAllCCTV(intensity)` — Cloudburst response
- `renderCCTVGrid()` — Render all 6 camera cards
- `renderCameraCanvas(id, status, camIndex)` — Canvas animation loop per camera

### Map & Layers (~300 lines)
- `refreshMap()` — Fetch /map/state, render ward choropleth, bind popups
- `loadHotspots()` — Fetch /map/hotspots (bbox-filtered, abortable)
- `toggleInfrastructure()` — Toggle hospital/substation/fire station markers
- `toggleElevation()` — Toggle SRTM elevation choropleth
- `toggleFloodline()` — Toggle Yamuna flood zones
- `toggleFtLayer(ft)` — Toggle flood type highlights (river/pluvial/backflow/sustained)

### Ward Drill-Down (~200 lines)
- `openDrillPanel(wardId, wardName)` — Slide-in panel + fetch detail
- `fetchWardDetail(wardId)` — GET /ward/{id}/detail
- `renderDrillDown(d)` — Populate stats, chart, infrastructure, summary
- `enrichDrillWithReadiness(wardId)` — Add readiness score + mechanisms
- `enrichDrillWithBacktest(wardId)` — Add 2023 backtest match

### Scoring & Cycle (~100 lines)
- `runCycle()` — POST /run/cycle → update map → dispatch QRT → refresh readiness
- `simulateCloudburst()` — Open modal → ingest rain → degrade → run cycle → spike sensors/CCTV
- `resetCity()` — POST /reset → restore baseline → reset QRT/CCTV

### Intelligence (~200 lines)
- `loadReadiness()` — Fetch /readiness/scores, cache in `_rpData`
- `loadForecast()` — Fetch /forecast/6hour, render bars
- `refreshYamuna()` — Fetch /yamuna/status, update strip
- `refreshSSI()` — Fetch /ssi, update gauges
- `refreshWeatherStatus()` — Fetch /weather/status, update header pill

### Backtest (~300 lines)
- `toggleBacktest()` — Show/hide backtest panel
- `runBacktest()` — POST /backtest/2023, poll status, render results
- `renderBacktest(d)` — Full results with metrics, confusion matrix, timeline, verdicts
- `applyBtMapOverlay(data)` — Color map TP/FP/FN/TN

### Radar (~400 lines)
- `_valNoise(x,y)` / `_fbm(x,y,oct)` — Noise functions
- `_dBZtoRGBA(dBZ)` — Color mapping
- `_renderRadarBase(W,H,frame)` — Offscreen cached base
- `drawRadarFrame(frame)` — Lightweight overlay (sweep + labels)
- `_radarLoop()` — rAF animation loop
- `toggleRadarPanel()` / `scrubRadar()` / `playRadar()` / `stopRadar()`

### UI Utilities
- `toggleTheme()` — Dark/light mode + tile swap
- `toggleSidePanel()` — Show/hide side panel
- `switchPanelTab(tabName)` — Tab switching (overview/risk/sensors/cctv)
- `togglePresentMode()` — Hide panels + controls for presentation
- `showAlert(type, title, msg)` — Toast notification system
- `updateAlertBanner()` — Dynamic risk summary banner
- `updateCountdown()` — Monsoon countdown timer
- `wardSearch(q)` — Autocomplete ward search
- `showDataSources()` — Open data sources modal
- `showSMSAlert(wardName, wsScore, floodProb, depth)` — SMS alert generation
- `computeFloodProbability(ws, elev, basin)` — Client-side probability estimate
- `computeInundationDepth(ws, elev, yamuna)` — Client-side depth estimate

---

## 12. Data Sources — What's Real vs Simulated

### ✅ REAL Data

| Data | Source | Confidence |
|------|--------|------------|
| 290 ward boundaries | Datameet Delhi + MCD/NDMC/Cantonment | High — official boundaries |
| SRTM elevation | NASA Shuttle Radar Topography Mission v4.1 | High — 30m satellite DEM |
| OSM infrastructure | OpenStreetMap Overpass API | Medium — crowdsourced |
| Drain names & locations (18 sensors) | PWD Delhi drain registry | High — real drain names |
| CWC warning/danger thresholds | Central Water Commission | High — validated against INDOFLOODS |
| Hathnikund travel time | CWC flood bulletins | High — historically verified |
| 2024 monsoon rainfall | Open-Meteo historical API | High — reanalysis data |
| 2023 flood event dates/levels | CWC Delhi Floods 2023 Case Study | High — official govt report |
| PMRS formula basis | IIT Delhi Drainage Master Plan 2018 | Medium — calibrated proxy |
| Runoff multiplier zones | NCR Functional Drainage Plan Table 5.4 | Medium — cited approximation |
| QRT depot locations (8) | PWD Delhi depot registry | High — real depot coordinates |
| CCTV drain locations (6) | PWD drain monitoring sites | High — real drain locations |

### ⟳ SIMULATED Data

| Data | What's Simulated | Notes |
|------|------------------|-------|
| Sensor water levels | Random walk with basin-weighted spikes | Production: PWD drain gauge API |
| CCTV camera feeds | Canvas-rendered drain scenes | Production: PWD CCTV API (47 cameras) |
| QRT vehicle GPS | Client-side animated dispatch | Production: GPS fleet tracking |
| Hotspot capacity values | Zone-randomized initial values | Production: PWD inspection data |
| LP pump assignments | Solved optimally from proxy data | Production: real fleet inventory |

### ≈ PROXY Data

| Data | Proxy Method | Error Range |
|------|-------------|-------------|
| Drain capacity (capacity_c) | Zone-averaged from IIT Delhi DMP | ±15% |
| Upstream discharge (Open-Meteo) | 1mm rain → 5,000 cusecs (empirical) | ±200–500% |
| Forecast triggered wards | Linear coefficients (unjustified) | Illustrative only |
| Yamuna stage prediction | Linear interpolation 202–208m | Ignores pool dynamics |

---

## 13. Deployment & Infrastructure

### Docker

```dockerfile
FROM python:3.11-slim
# System: libpq-dev, gcc, GDAL
# Python: requirements.txt
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Render (render.yaml)

```yaml
services:
  - type: web
    name: flood-ready-delhi
    env: docker
    plan: free
    envVars:
      - key: DATABASE_URL
        fromDatabase: flood-db
```

### Docker Compose (local dev)

```yaml
services:
  db:     postgres:16 + PostGIS 3.4
  app:    ./backend (port 8000)
  tileserv: pg_tileserv (port 7800)
```

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | postgresql://hydro:hydro123@localhost:5432/hydrology | PostgreSQL connection |
| `API_SECRET_KEY` | hydro-mvp-secret-2026 | X-API-Key auth |
| `FRONTEND_DIR` | /app/frontend | Static file root |
| `RAIN_POLL_INTERVAL_SECONDS` | 600 | Weather poll frequency |
| `OWM_API_KEY` | (optional) | OpenWeatherMap API key |
| `RENDER_EXTERNAL_URL` | (auto) | Self-ping keep-alive URL |
| `CITY_LAT` / `CITY_LON` | 28.65 / 77.22 | Delhi center coordinates |

---

## 14. Dependencies & Libraries

### Backend (Python)

| Package | Version | Purpose |
|---------|---------|---------|
| fastapi | 0.111.0 | Web framework |
| uvicorn[standard] | 0.29.0 | ASGI server |
| sqlalchemy[asyncio] | 2.0.30 | ORM + async engine |
| geoalchemy2 | 0.15.1 | PostGIS SQLAlchemy types |
| asyncpg | 0.29.0 | PostgreSQL async adapter |
| psycopg[binary] | 3.1.19 | PostgreSQL synchronous + async |
| psycopg2-binary | ≥2.9.9 | Legacy scripts (seed) |
| pydantic | 2.7.1 | Request/response validation |
| pulp | 2.8.0 | Integer linear programming (dispatch) |
| shapely | 2.0.4 | Geometry WKT↔GeoJSON conversion |
| httpx | 0.27.0 | Async HTTP client (tile proxy) |
| apscheduler | 3.10.4 | Background job scheduling |
| aiofiles | 23.2.1 | Async file operations |
| python-dotenv | 1.0.1 | .env loading |

### Frontend (CDN)

| Library | Version | Purpose |
|---------|---------|---------|
| Leaflet | 1.9.4 | Interactive map |
| Leaflet.VectorGrid | 1.3.0 | Vector tile layer |
| Leaflet.heat | 0.2.0 | Heatmap overlay |
| Chart.js | 4.4.0 | Line/bar/area charts |
| chartjs-plugin-annotation | 3.0.1 | Chart threshold lines |
| DM Sans + DM Mono | (Google Fonts) | Typography |

---

## 15. Key Constants & Thresholds (Reference Table)

### Scoring Engine

| Constant | Value | Source |
|----------|-------|--------|
| `W_PLUVIAL` | 0.45 | IIT Delhi DMP weighting |
| `W_FLUVIAL` | 0.30 | IIT Delhi DMP weighting |
| `W_COMPOUND` | 0.25 | IIT Delhi DMP weighting |
| `PLUVIAL_SCALE` | 18.5 | Normalizes C/(R×T) to 0–100 |
| `RAIN_WINDOW_MIN` | 180 | 3-hour accumulation |
| `TRIGGER_SCORE` | 65 | Below = triggered |
| `CRITICAL_SCALE` | 20.0 | Normalizes critical_penalty_pc |
| `HORTON_F_INF` | 0.635 mm/hr | Group D loam (IIT Delhi Table 4.3-1) |
| `HORTON_F_0` | 76.2 mm/hr | Initial dry capacity |
| `HORTON_K_DECAY` | 0.00115 s⁻¹ | Infiltration decay |

### Yamuna Thresholds (Old Railway Bridge)

| Level | Elevation | Source |
|-------|-----------|--------|
| Normal | <202.0m | CWC |
| Warning | 204.22m | CWC + INDOFLOODS validated |
| Danger | 205.33m | CWC + INDOFLOODS validated |
| High Flood | 206.0m | CWC |
| Extreme | 208.0m | CWC (2023 peak: 208.66m) |

### Hathnikund Discharge Thresholds

| Level | Cusecs | Travel Time |
|-------|--------|-------------|
| Normal | <100,000 | 72 hours |
| Watch | 100,000 | 60 hours |
| Warning | 200,000 | 56 hours |
| Danger | 300,000 | 52 hours |
| Extreme | 500,000 | 48 hours |

### Terrain Classification (SRTM Slope)

| Class | Slope | Runoff T |
|-------|-------|----------|
| Floodplain | <0.5° | 3.0+ |
| Flat | 0.5–1° | 1.3 |
| Moderate | 1–2° | 1.7 |
| Steep | 2–3° | 2.2 |
| Ridge | >3° | 2.7–3.5 |

### Ward Color Bands (UI)

| Band | Score Range | Fill | Border |
|------|------------|------|--------|
| Critical | 0–40 | #FEE2E2 | #DC2626 |
| At Risk | 40–65 | #FEF3C7 | #D97706 |
| Ready | 65–100 | #DCFCE7 | #16A34A |

---

## 16. Known Limitations

### Data Gaps
- Drain capacity (`capacity_c`) is zone-averaged proxy, not direct measurement
- Upstream discharge proxy (1mm = 5,000 cusecs) has no hydrological basis
- Forecast ward-trigger coefficients are unjustified linear approximations
- Backtest reference classification uses name-substring matching, not satellite imagery
- OSM infrastructure includes some NCR (Gurgaon) facilities

### Technical Limitations
- Single monolithic HTML file (8,128 lines) — no component framework
- No build step — no minification, tree-shaking, or code splitting
- Sensor/CCTV data is client-side simulated (no actual IoT integration)
- IMD scraping is regex-based and fragile to website changes
- Backtest job state is lost on Render restart (module-level variable)
- No pagination on history endpoints (could return 10k+ rows)

### Model Uncertainty
- PMRS formula weights (45/30/25) partially cited but no sensitivity analysis
- ±15% uncertainty on all proxy-input-based scores
- SSI rain saturation threshold (50mm) unjustified
- C/(R×T) scoring has no direct hydrological basis (not SCS-CN or rational method)

### Security
- Default API key is hardcoded ('hydro-mvp-secret-2026')
- `/ingest/sensor` has no authentication
- No rate limiting on any endpoint
- No input sanitization on ward search (client-side only)

---

*Generated: March 2026 — FloodReady Delhi v1.0 MVP*
*Total backend: ~40 Python files, ~3,500 lines of code*
*Total frontend: 1 HTML file, 8,128 lines (CSS + JS)*
