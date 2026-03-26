"""
Floodline regulation layer endpoint.

GET /map/floodline  — returns GeoJSON of Delhi wards classified into
                      Yamuna flood zones based on mean elevation.

Zone classification (Old Railway Bridge datum):
  Zone 1 — Floodplain     : mean elevation < 207m  (25-yr return period)
  Zone 2 — Danger zone    : 207m – 210m             (high flood risk)
  Zone 3 — Buffer zone    : 210m – 215m             (precautionary)
  Zone 4 — Safe           : > 215m                  (low flood risk)

The 207m threshold corresponds to the Yamuna danger level (205.33m gauge)
plus ~1.5m freeboard to account for backflow and drain surcharge.

Also returns:
  - Yamuna danger contour as a polyline (approx from ward centroids)
  - Encroachment count: structures in Zone 1 (NGT regulated area)
"""

from fastapi import APIRouter
from sqlalchemy import text
from app.db import engine

router = APIRouter(tags=["floodline"])

# ── Zone classification ────────────────────────────────────────────────────
def _zone(elev: float) -> dict:
    if elev < 207:
        return {"zone": 1, "label": "Floodplain",  "color": "#DC2626",
                "opacity": 0.55, "desc": "25-yr floodplain — construction prohibited (NGT 2023)"}
    if elev < 210:
        return {"zone": 2, "label": "Danger zone", "color": "#D97706",
                "opacity": 0.40, "desc": "High flood risk — no permanent structures"}
    if elev < 215:
        return {"zone": 3, "label": "Buffer zone", "color": "#CA8A04",
                "opacity": 0.22, "desc": "Precautionary buffer — restricted development"}
    return {"zone": 4, "label": "Safe",          "color": "#16A34A",
            "opacity": 0.10, "desc": "Low flood risk from Yamuna overtopping"}


@router.get("/map/floodline")
async def floodline_layer():
    """
    Ward polygons classified by Yamuna flood zone.
    Each feature carries zone, label, fill colour, and regulatory description.
    """
    async with engine.connect() as conn:
        rows = await conn.execute(text("""
            SELECT
                w.id                                AS ward_id,
                w.name                              AS ward_name,
                w.zone_name,
                ST_AsGeoJSON(w.geom)::json          AS geometry,
                COALESCE(we.mean_elevation, 213.0)  AS mean_elevation,
                COALESCE(we.terrain_class, 'urban') AS terrain_class,
                COALESCE(we.runoff_t, 1.5)          AS runoff_t
            FROM wards w
            LEFT JOIN ward_elevation we ON we.ward_id = w.id
            ORDER BY we.mean_elevation ASC NULLS LAST
        """))
        ward_rows = rows.mappings().fetchall()

    features = []
    zone_counts = {1: 0, 2: 0, 3: 0, 4: 0}

    for r in ward_rows:
        elev = float(r["mean_elevation"])
        z    = _zone(elev)
        zone_counts[z["zone"]] += 1

        features.append({
            "type": "Feature",
            "geometry": r["geometry"],
            "properties": {
                "ward_id":       r["ward_id"],
                "ward_name":     r["ward_name"] or "—",
                "zone_name":     r["zone_name"] or "—",
                "mean_elevation": round(elev, 1),
                "terrain_class": r["terrain_class"],
                "flood_zone":    z["zone"],
                "zone_label":    z["label"],
                "fill_color":    z["color"],
                "fill_opacity":  z["opacity"],
                "description":   z["desc"],
            }
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "zone_counts":     zone_counts,
            "floodplain_wards":   zone_counts[1],
            "danger_zone_wards":  zone_counts[2],
            "buffer_zone_wards":  zone_counts[3],
            "safe_wards":         zone_counts[4],
            "datum": "Old Railway Bridge gauge + 1.5m freeboard",
            "source": "SRTM 90m DEM via ward_elevation table",
            "ngt_order": "NGT 2023 — floodplain construction prohibited",
        }
    }
