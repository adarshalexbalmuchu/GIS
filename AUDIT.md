# FloodReady Delhi — Project Audit
*Last updated: 2026-03-24 — reflects changes in commit 716c596*

---

## 1. What's WORKING (End-to-End)

| Feature | Status | Notes |
|---|---|---|
| Rain ingestion → hotspot scoring → pump dispatch | ✅ Working | Full pipeline: POST → score 290 wards → LP solver → WebSocket broadcast |
| Interactive map (ward choropleth + hotspots) | ✅ Working | Leaflet + PostGIS GeoJSON with color-coded PMRS bands |
| Cloudburst simulation | ✅ Working | Delhi bbox fallback added — no longer fails if /city/bounds returns 500 |
| Pre-monsoon readiness scoring (PMRS) | ✅ Working | Static risk index for all 290 wards with red/amber/green bands |
| Ward drill-down detail | ✅ Working | Elevation, infrastructure, 30-day score history, export PDF |
| Yamuna advance warning | ✅ Working | Hathnikund discharge → Delhi stage estimate → ETA |
| System Saturation Index (SSI) | ✅ Working | Composite 0–100 stress score with 5-day lookback |
| 2023 monsoon replay (backtest) | ✅ Working | 6-event replay with precision/recall/F1 |
| Export HTML reports | ✅ Working | MCD-branded ward cards and league table |
| History timeline (monsoon 2024) | ✅ Working | Real Open-Meteo data for 122-day season |
| Weather polling (IMD + OWM) | ✅ Working | Auto-fetch every 10 min, auto-trigger cycles |
| WebSocket live updates | ✅ Working | Broadcasts cycle results + SSI to all connected clients |
| Render deploy (auto-seed) | ✅ Working | Zero-touch first deploy with 290 wards + hotspots + elevation + history |
| Flood type classification (river/backflow/sustained/pluvial) | ✅ Working | Fixed — now uses runoff_t as primary signal; no longer requires cycle history |
| Methodology modal with citations | ✅ Working | 6 IIT Delhi / INDOFLOODS / DDMA citation cards added |

---

## 2. What's REAL vs ESTIMATED/FABRICATED

### Real Data Sources

| Data | Source | Verified |
|---|---|---|
| Ward boundaries (290) | Datameet Delhi + MCD/NDMC/Cantonment | ✅ Real official data |
| Elevation (SRTM v4.1) | NASA Shuttle Radar Topography Mission | ✅ Real satellite DEM |
| OSM infrastructure | OpenStreetMap hospitals/substations/fire stations | ✅ Real crowdsourced |
| Live weather | OpenWeatherMap API (temp, rain, conditions) | ✅ Real API |
| IMD rainfall | India Meteorological Department city page scraping | ✅ Real govt source |
| Yamuna discharge thresholds | CWC published alert levels (100k/200k/300k cusecs) + INDOFLOODS gauge-164 validation | ✅ Real official + validated |
| Travel time Hathnikund→Delhi | CWC flood bulletins (48–72 hours) | ✅ Real historical |
| Monsoon 2024 rainfall | Open-Meteo historical API (122 days) | ✅ Real climate data |
| 2023 flood dates/levels | CWC Delhi Floods 2023 Case Study (Aug 2024) — peak 208.66m Jul 13, peak discharge 3,60,000 cusecs Jul 11 | ✅ Real, cited |
| Yamuna warning/danger thresholds (204.5m / 205.33m) | Validated against INDOFLOODS gauge INDOFLOODS-gauge-164 (IIT Delhi, 2025, DOI: 10.5281/zenodo.14584654) — 48 years of records 1963–2011 | ✅ Validated |
| Drain capacity proxy formula | Calibrated against IIT Delhi Drainage Master Plan for NCT Delhi, I&FC Dept, 2018 (SWMM sub-catchment analysis, Chapter 3) | ✅ Cited approximation |
| Runoff multiplier (runoff_t) zone values | Derived from NCR Functional Drainage Plan, Table 5.4–5.5, NCRPB | ✅ Cited approximation |
| Four-type flood classification | Validated against DDMA documentation of 18-drain backflow mechanism | ✅ Cited |
| Backflow type (drain reversal) | DDMA-documented: 18 major Delhi drains experience reverse flow from Yamuna during spate | ✅ Real mechanism |

### Estimated / Fabricated (Remaining)

| Item | What's Wrong | Impact |
|---|---|---|
| Upstream discharge proxy: 1mm → 5,000 cusecs | Completely made up. No hydrological basis. Real relationship depends on catchment area, soil, slope | Yamuna estimates could be off by 2–5× when CWC is down |
| Forecast: rainfall → triggered wards (4, 5, 4, 2 wards/mm) | Linear coefficients with no source. Ignores drainage capacity, elevation | Forecast panel shows fabricated ward-trigger predictions |
| Yamuna stage prediction: linear interpolation 202–208m | Ignores pool dynamics, dam gate operations, tributary confluence | Stage estimate unreliable at moderate discharge |
| Backtest intensity values: 9.8, 8.5, 6.2 mm/hr | No source cited. Seemingly derived from daily totals ÷ assumed hours | Backtest replay uses questionable intensities |
| Backtest reference flooded wards: substring name matching | "Yamuna Apartments" in South Delhi would match. Based on news reports, not satellite/survey | Precision/Recall metrics illustrative rather than verified; INDOFLOODS dataset (Zenodo) available to replace this |
| LP pump caps: 15 pumps/ward, 3 pumps/hotspot | No source. Are 15 pumps realistic for Delhi MCD logistics? | Dispatch recommendations aren't actionable |
| PMRS formula weights: 40/30/20/10 | Weighted highest flood risk per NDMA Urban Flood Guidelines 2010 (cited in modal), but exact split unjustified | Core scoring model is partially justified |
| SSI rain saturation threshold: 50mm | Why 50mm? No hydrological paper cited | SSI caps out at moderate rainfall |
| Scoring trigger threshold: ws_score < 70 | Arbitrary cutoff. No sensitivity analysis | Could miss or over-trigger wards |

---

## 3. BUGS & Issues

### Fixed in commit 716c596

| Bug | Location | Fix Applied |
|---|---|---|
| Cloudburst — wards don't turn red | `frontend/index.html` — `simulateCloudburst()` | Added Delhi bbox fallback; /city/bounds 500 no longer kills function before runCycle() fires |
| Flood type filters — all Pluvial | `backend/app/services/readiness.py` — `_flood_type()` | Added runoff_t as primary classification signal (rt ≥ 2.8 → backflow, rt ≥ 3.0 → sustained); raised elevation threshold 210→215m; works without historical cycle data |
| Popup elevation showing "—" | `frontend/index.html` — `onEachFeature` in `refreshMap()` | terrainLine now gates on `(p.terrain_class \|\| p.mean_elevation)`; elevation renders independently |
| Drill panel elevation showing "—" | `frontend/index.html` — `renderDrillDown()` | Changed truthy checks to `!= null` for both `mean_elevation` and `runoff_t` |

### Critical (Red) — Still Open

| Bug | Location | Impact |
|---|---|---|
| Ward with no hotspots = always "safe" | `scoring.py` | Unmapped wards never trigger — false sense of security |
| Hotspot cache stale after /run/cycle | `map_state.py` | Map shows outdated capacity values until restart/manual reload |
| No user-facing error toasts for cycle failures | `frontend/index.html` | Failed cycles/resets silently log to removed console — users see nothing |
| Hospital/fire station data includes NCR | `hospitals.json` | Gurgaon hospitals counted as Delhi infrastructure |

### Medium (Amber) — Still Open

| Bug | Location | Impact |
|---|---|---|
| IMD HTML scraping is fragile (regex-based) | `weather.py` | IMD website redesign silently breaks rainfall fetch |
| IMD 24h rain averaged to mm/hr | `weather.py` | 12mm in 24h becomes 0.5mm/hr — destroys peak intensity signal |
| Single rain polygon for entire Delhi | `weather.py` | No spatial variation — entire city treated as uniform rain |
| /ingest/sensor has no auth | `ingest.py` | Anyone can inject fake sensor readings |
| Backtest job lost on redeploy | `backtest.py` | Module-level state — result gone if Render restarts |
| `geo.py` is empty placeholder | `geo.py` | No geospatial utility functions |
| `models.py` and `schemas.py` are stubs | `backend/app/` | All models in db.py — confusing structure |

### Low — Still Open

| Bug | Location | Impact |
|---|---|---|
| `ward_name` field blank in elevation JSON | `delhi_ward_elevation.json` | Data quality gap |
| Orphaned UI components (compare panel, drawer) | Frontend | Dead code, incomplete mobile UX |
| No pagination on `/history/*` endpoints | `map_state.py` | Could return 10k+ rows |
| `except Exception: pass` in weather poll | `main.py` | Masks real errors silently |

---

## 4. Formulas & Models — Credibility Assessment

### PMRS (Pre-Monsoon Readiness Score) — Core Model
**Verdict:** Formula structure is sound (weighted multi-criteria). Weights now partially cited against NDMA Urban Flood Guidelines 2010 in the methodology modal. Drain capacity proxy now cited against IIT Delhi Drainage Master Plan 2018. Still no sensitivity analysis.

### Ward Scoring (Live Cycle)
**Verdict:** Inverse relationship with rainfall (higher rain = lower score) makes sense conceptually. The specific formula C/(R×T) has no hydrological basis — not SCS-CN, not rational method. Remains a known gap.

### LP Dispatch — Strongest Component
**Verdict:** Clean integer LP formulation using PuLP. Most technically defensible component. Unchanged.

### SSI (System Saturation Index)
**Verdict:** Weights unjustified. Naively sums rain intensity over 5 days instead of tracking peak 6-hour intensity. Unchanged.

### Flood Type Classification
**Verdict:** Now defensible. runoff_t as primary signal maps to NCR Functional Drainage Plan zone imperviousness values. Backflow type validated against DDMA 18-drain reversal documentation. Elevation threshold 215m aligns with IIT Delhi DMP transition zone.

### Yamuna Thresholds
**Verdict:** Now validated. Warning 204.5m and danger 205.33m confirmed against INDOFLOODS gauge-164 (48 years of records). This is the strongest validation we have.

---

## 5. Remaining Improvements

### Quick Wins (< 1 hour each)

| Item | Status |
|---|---|
| Add provenance comments to hardcoded constants | ✅ Done — backtest.py, readiness.py, methodology modal |
| Fix "no hotspots = safe" bug | ⬜ Open |
| Invalidate hotspot cache after cycle | ⬜ Open |
| Filter OSM data to Delhi boundary | ⬜ Open |
| Add error toast for cycle failures | ⬜ Open |

### Medium Effort

- Replace linear forecast coefficients with backtest-derived numbers
- Document upstream discharge proxy with proper citation and error range
- Add confidence intervals to predictions (e.g. "4 ± 3 wards, 80% CI")
- Sensitivity analysis: show F1 change across weight variants (30/30/20/20 vs 40/30/20/10)
- **Replace substring ward matching in backtest with INDOFLOODS spatial join** — dataset already downloaded, this is the highest-credibility remaining fix

### High Impact (Differentiators)

- Swap scoring formula to SCS-CN method (USDA Soil Conservation Service Curve Number) — industry standard for urban runoff
- Multi-point rainfall grid — 5×5 grid with per-cell intensity from OpenWeatherMap multi-location queries
- Add ground-truth validation table with named wards and outcomes

---

## 6. Citations Added (this session)

| Claim | Source |
|---|---|
| Drain capacity proxy formula | IIT Delhi Drainage Master Plan for NCT Delhi, I&FC Dept, 2018, Chapter 3 (SWMM analysis). ifc.delhi.gov.in |
| Runoff multiplier zone calibration | NCR Functional Drainage Plan, Table 5.4–5.5, NCRPB. ncrpb.nic.in |
| Four-type flood classification + backflow mechanism | Delhi Disaster Management Authority, Flood Mechanism Documentation. ddma.delhi.gov.in |
| System positioning | IIT Delhi HydroSense Lab, Barapullah Early Warning System. jalsuraksha.iitd.ac.in/barapullah |
| 2023 backtest event data | CWC Delhi Floods 2023 Case Study, August 2024. cwc.gov.in |
| Yamuna threshold validation | INDOFLOODS gauge-164 "Delhi Railway Bridge", IIT Delhi HydroSense Lab, 2025. DOI: 10.5281/zenodo.14584654 |
| Catchment / basin attributes | INDOFLOODS catchment_characteristics dataset. DOI: 10.5281/zenodo.14584654 |

---

**Bottom line:** The project is impressive in scope — 290 real wards, PostGIS spatial queries, LP dispatch, WebSocket, 2023 backtest, Yamuna advance warning. The architecture is production-grade. This session addressed the three most visible runtime bugs (cloudburst, flood-type filters, elevation display) and grounded the core model constants in seven published sources. The main remaining weakness is the backtest precision/recall metrics, which still rely on ward-name substring matching rather than the INDOFLOODS spatial dataset.
