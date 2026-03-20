"""
MCD Ward Export — printable HTML summary card.

GET /export/ward/{ward_id}   — returns a self-contained HTML page
                               suitable for printing or saving as PDF.
GET /export/summary          — returns a printable A4 league table of
                               all wards ranked by readiness score.

The export pages are designed for:
  - Field engineers carrying a tablet or printed sheet
  - MCD command room wall displays
  - Hackathon/presentation handouts

Each ward card includes:
  - Readiness score with colour band
  - Flood type classification
  - Key metrics: elevation, hotspots, infrastructure count
  - 30-day trigger history
  - Pre-monsoon action items based on score band
  - QR-friendly URL for field use
"""

from datetime import datetime
from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from sqlalchemy import text

from app.db import engine
from app.services.readiness import compute_readiness_scores

router = APIRouter(prefix="/export", tags=["export"])

# ── Colour helpers ────────────────────────────────────────────────────────
_BAND_COLOURS = {
    "red":   {"bg": "#FEE2E2", "border": "#DC2626", "text": "#991B1B", "label": "CRITICAL"},
    "amber": {"bg": "#FEF3C7", "border": "#D97706", "text": "#92400E", "label": "AT RISK"},
    "green": {"bg": "#DCFCE7", "border": "#16A34A", "text": "#14532D", "label": "READY"},
}

_FT_LABELS = {
    "river":     "River / Yamuna flood",
    "pluvial":   "Pluvial / Rainfall flash flood",
    "backflow":  "Drain backflow flood",
    "sustained": "Sustained rainfall flood",
}

_ACTION_ITEMS = {
    "red": [
        "Immediate desilt of all ward drains before June 15",
        "Verify all pump stations — test run required",
        "Pre-position NDRF team and flood relief materials",
        "Issue advance notice to residents in low-lying areas",
        "Confirm evacuation routes and nearest shelter capacity",
    ],
    "amber": [
        "Complete drain desilting within 3 weeks",
        "Pump station inspection and maintenance this week",
        "Update ward flood contact list and alert tree",
        "Mark waterlogging-prone roads on ward map",
        "Confirm medical facility flood preparedness",
    ],
    "green": [
        "Routine monthly drain inspection — maintain current state",
        "Verify sensor coverage and report any offline units",
        "Ward flood contact list review",
        "Update ward boundary map with new construction",
    ],
}


def _ward_card_html(ward: dict, detail: dict) -> str:
    """Generate a self-contained HTML card for a single ward."""
    band   = ward.get("band", "green")
    colours = _BAND_COLOURS.get(band, _BAND_COLOURS["green"])
    score  = ward.get("readiness_score", 0)
    ft     = ward.get("flood_type", "pluvial")
    ni     = detail.get("nearby_infrastructure", {})
    hist   = detail.get("score_history", [])
    actions = _ACTION_ITEMS.get(band, _ACTION_ITEMS["green"])

    # Build mini sparkline data
    spark_vals = [h["ws_score"] for h in hist[-14:] if h.get("ws_score") is not None]
    spark_max  = max(spark_vals, default=100)
    spark_min  = min(spark_vals, default=0)
    spark_range = max(spark_max - spark_min, 1)
    spark_pts  = " ".join(
        f"{i * 12},{40 - int(((v - spark_min) / spark_range) * 36)}"
        for i, v in enumerate(spark_vals)
    ) if spark_vals else ""

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ward Report — {ward.get('ward_name','—')}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Inter', sans-serif; background: #F9FAFB;
    color: #111827; padding: 24px; max-width: 720px; margin: 0 auto;
  }}
  @media print {{
    body {{ padding: 0; background: white; }}
    .no-print {{ display: none !important; }}
  }}
  .header {{ display: flex; justify-content: space-between; align-items: flex-start;
             margin-bottom: 20px; padding-bottom: 16px; border-bottom: 2px solid #E5E7EB; }}
  .mcd-logo {{ font-size: 11px; font-weight: 600; color: #6B7280;
               text-transform: uppercase; letter-spacing: .08em; }}
  .mcd-logo span {{ display: block; font-size: 16px; font-weight: 700; color: #111827; }}
  .report-meta {{ text-align: right; font-size: 11px; color: #6B7280; }}
  .ward-hero {{ background: {colours['bg']}; border: 2px solid {colours['border']};
                border-radius: 12px; padding: 20px 24px; margin-bottom: 20px;
                display: flex; justify-content: space-between; align-items: center; }}
  .ward-name {{ font-size: 22px; font-weight: 700; color: {colours['text']}; }}
  .ward-zone {{ font-size: 13px; color: {colours['text']}; opacity: .75; margin-top: 3px; }}
  .score-circle {{ width: 72px; height: 72px; border-radius: 50%;
                   border: 3px solid {colours['border']};
                   background: white; display: flex; flex-direction: column;
                   align-items: center; justify-content: center; flex-shrink: 0; }}
  .score-num {{ font-size: 22px; font-weight: 700; color: {colours['text']}; line-height: 1; }}
  .score-lbl {{ font-size: 9px; color: {colours['text']}; font-weight: 600;
                text-transform: uppercase; letter-spacing: .06em; }}
  .band-badge {{ display: inline-block; font-size: 12px; font-weight: 700;
                 padding: 3px 12px; border-radius: 20px; margin-top: 8px;
                 background: {colours['border']}; color: white; letter-spacing: .06em; }}
  .section {{ margin-bottom: 18px; }}
  .section-title {{ font-size: 11px; font-weight: 600; color: #6B7280;
                    text-transform: uppercase; letter-spacing: .06em;
                    margin-bottom: 10px; padding-bottom: 4px;
                    border-bottom: 1px solid #E5E7EB; }}
  .metrics-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }}
  .metric {{ background: white; border: 1px solid #E5E7EB; border-radius: 8px;
             padding: 10px 12px; text-align: center; }}
  .metric-val {{ font-size: 20px; font-weight: 700; color: #111827; }}
  .metric-lbl {{ font-size: 10px; color: #6B7280; margin-top: 2px; }}
  .ft-row {{ display: flex; align-items: center; gap: 10px;
             background: white; border: 1px solid #E5E7EB; border-radius: 8px;
             padding: 10px 14px; }}
  .ft-dot {{ width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }}
  .ft-name {{ font-size: 13px; font-weight: 500; }}
  .ft-desc {{ font-size: 11px; color: #6B7280; margin-left: auto; }}
  .action-list {{ list-style: none; }}
  .action-list li {{ display: flex; gap: 10px; padding: 7px 0;
                     border-bottom: 1px solid #F3F4F6; font-size: 12px; }}
  .action-list li:last-child {{ border-bottom: none; }}
  .action-num {{ width: 20px; height: 20px; border-radius: 50%; flex-shrink: 0;
                 background: {colours['border']}; color: white;
                 font-size: 10px; font-weight: 700; display: flex;
                 align-items: center; justify-content: center; }}
  .sparkline {{ width: 100%; height: 44px; }}
  .infra-row {{ display: flex; gap: 12px; }}
  .infra-badge {{ flex: 1; background: white; border: 1px solid #E5E7EB;
                  border-radius: 8px; padding: 8px 12px; text-align: center; }}
  .infra-num {{ font-size: 18px; font-weight: 700; }}
  .infra-lbl {{ font-size: 10px; color: #6B7280; }}
  .footer {{ margin-top: 24px; padding-top: 12px; border-top: 1px solid #E5E7EB;
             font-size: 10px; color: #9CA3AF; display: flex; justify-content: space-between; }}
  .print-btn {{ position: fixed; bottom: 24px; right: 24px; padding: 10px 20px;
                background: #2563EB; color: white; border: none; border-radius: 8px;
                font-size: 13px; font-weight: 600; cursor: pointer; box-shadow: 0 4px 12px rgba(37,99,235,.3); }}
  .print-btn:hover {{ background: #1D4ED8; }}
</style>
</head>
<body>

<div class="header">
  <div class="mcd-logo">
    Municipal Corporation of Delhi
    <span>Urban Flood Intelligence System</span>
  </div>
  <div class="report-meta">
    Pre-Monsoon Ward Report<br>
    Generated: {now_str}<br>
    <strong>OFFICIAL USE ONLY</strong>
  </div>
</div>

<div class="ward-hero">
  <div>
    <div class="ward-name">{ward.get('ward_name','—')}</div>
    <div class="ward-zone">{ward.get('zone_name','—')} · Ward No. {ward.get('ward_no','—')}</div>
    <span class="band-badge">{colours['label']}</span>
  </div>
  <div class="score-circle">
    <div class="score-num">{score:.0f}</div>
    <div class="score-lbl">/100</div>
  </div>
</div>

<div class="section">
  <div class="section-title">Key Metrics</div>
  <div class="metrics-grid">
    <div class="metric">
      <div class="metric-val">{detail.get('mean_elevation','—')}{'m' if detail.get('mean_elevation') else ''}</div>
      <div class="metric-lbl">Elevation</div>
    </div>
    <div class="metric">
      <div class="metric-val">{detail.get('hotspot_count','—')}</div>
      <div class="metric-lbl">Hotspots</div>
    </div>
    <div class="metric">
      <div class="metric-val">{detail.get('times_triggered_30d','—')}</div>
      <div class="metric-lbl">Triggered (30d)</div>
    </div>
    <div class="metric">
      <div class="metric-val">{ward.get('trigger_rate_pct','—')}{'%' if ward.get('trigger_rate_pct') is not None else ''}</div>
      <div class="metric-lbl">Trigger rate</div>
    </div>
  </div>
</div>

<div class="section">
  <div class="section-title">Dominant Flood Type</div>
  <div class="ft-row">
    <div class="ft-dot" style="background:{'#2563EB' if ft=='river' else '#D97706' if ft=='pluvial' else '#7C3AED' if ft=='backflow' else '#DC2626'}"></div>
    <div class="ft-name">{_FT_LABELS.get(ft, ft.title())}</div>
    <div class="ft-desc">Terrain: {detail.get('terrain_class','—')} · Runoff: {detail.get('runoff_t','—')}×</div>
  </div>
</div>

<div class="section">
  <div class="section-title">Critical Infrastructure within 500m</div>
  <div class="infra-row">
    <div class="infra-badge">
      <div class="infra-num" style="color:#DC2626">{ni.get('hospitals',0)}</div>
      <div class="infra-lbl">Hospitals</div>
    </div>
    <div class="infra-badge">
      <div class="infra-num" style="color:#D97706">{ni.get('substations',0)}</div>
      <div class="infra-lbl">Substations</div>
    </div>
    <div class="infra-badge">
      <div class="infra-num" style="color:#2563EB">{ni.get('fire_stations',0)}</div>
      <div class="infra-lbl">Fire Stations</div>
    </div>
  </div>
</div>

{'<div class="section"><div class="section-title">30-Day Score History (PMRS)</div><svg class="sparkline" viewBox="0 0 ' + str(len(spark_vals)*12) + ' 44" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none"><polyline points="' + spark_pts + '" fill="none" stroke="' + colours["border"] + '" stroke-width="1.5" stroke-linejoin="round"/></svg></div>' if spark_pts else ''}

<div class="section">
  <div class="section-title">Pre-Monsoon Action Items</div>
  <ul class="action-list">
    {''.join(f'<li><span class="action-num">{i+1}</span>{item}</li>' for i, item in enumerate(actions))}
  </ul>
</div>

<div class="footer">
  <span>Urban Hydrology Engine — MCD Flood Intelligence System</span>
  <span>Ward ID: {ward.get('ward_id','—')} · /export/ward/{ward.get('ward_id','—')}</span>
</div>

<button class="print-btn no-print" onclick="window.print()">🖨 Print / Save PDF</button>

</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────

@router.get("/ward/{ward_id}", response_class=HTMLResponse)
async def export_ward(ward_id: int):
    """Printable HTML ward summary card."""
    async with engine.connect() as conn:
        # Get readiness data
        scores = await compute_readiness_scores(conn)
        ward   = next((s for s in scores if s["ward_id"] == ward_id), None)
        if not ward:
            return HTMLResponse(f"<h3>Ward {ward_id} not found</h3>", status_code=404)

        # Get detail
        det_row = await conn.execute(text("""
            SELECT w.id, w.name, w.zone_name, w.ward_no,
                   we.terrain_class, we.mean_elevation, we.runoff_t
            FROM wards w
            LEFT JOIN ward_elevation we ON we.ward_id = w.id
            WHERE w.id = :wid
        """), {"wid": ward_id})
        det = det_row.mappings().first()

        hs_row = await conn.execute(text("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE critical_penalty_pc > 0) AS critical
            FROM hotspots WHERE ward_id = :wid
        """), {"wid": ward_id})
        hs = hs_row.mappings().first()

        infra_row = await conn.execute(text("""
            SELECT ci.facility_type, COUNT(DISTINCT ci.id) AS cnt
            FROM hotspots h
            JOIN critical_infrastructure ci
              ON ST_DWithin(ci.geom::geography, h.geom::geography, 500)
            WHERE h.ward_id = :wid
            GROUP BY ci.facility_type
        """), {"wid": ward_id})
        infra_map = {r["facility_type"]: r["cnt"] for r in infra_row.mappings().all()}

        hist_row = await conn.execute(text("""
            SELECT created_at::date AS day, AVG(ws_score) AS ws_score
            FROM dispatch_runs WHERE ward_id = :wid
            GROUP BY created_at::date ORDER BY day DESC LIMIT 30
        """), {"wid": ward_id})
        history = [{"date": str(r["day"]), "ws_score": float(r["ws_score"])}
                   for r in hist_row.mappings().all()]

        trig_row = await conn.execute(text("""
            SELECT COUNT(*) FILTER (WHERE status IN ('critical','dispatched','triggered'))
            AS triggered FROM dispatch_runs WHERE ward_id = :wid
        """), {"wid": ward_id})
        times_triggered = trig_row.scalar_one() or 0

    detail = {
        "mean_elevation":     round(float(det["mean_elevation"]), 1) if det and det["mean_elevation"] else None,
        "terrain_class":      det["terrain_class"] if det else "—",
        "runoff_t":           round(float(det["runoff_t"]), 1) if det and det["runoff_t"] else None,
        "hotspot_count":      int(hs["total"]) if hs else 0,
        "nearby_infrastructure": {
            "hospitals":     infra_map.get("hospital", 0),
            "substations":   infra_map.get("substation", 0),
            "fire_stations": infra_map.get("fire_station", 0),
        },
        "score_history":      list(reversed(history)),
        "times_triggered_30d": int(times_triggered),
    }

    return HTMLResponse(_ward_card_html(ward, detail))


@router.get("/summary", response_class=HTMLResponse)
async def export_summary():
    """Printable A4 league table — all wards ranked by readiness score."""
    async with engine.connect() as conn:
        scores = await compute_readiness_scores(conn)

    now_str = datetime.now().strftime("%d %b %Y, %H:%M IST")
    red   = [s for s in scores if s["band"] == "red"]
    amber = [s for s in scores if s["band"] == "amber"]
    green = [s for s in scores if s["band"] == "green"]

    def _rows(wards, band):
        c = _BAND_COLOURS[band]
        rows = ""
        for i, w in enumerate(wards):
            rows += f"""<tr>
              <td style="color:#6B7280;font-size:11px">{i+1}</td>
              <td><a href="/export/ward/{w['ward_id']}" style="color:inherit;text-decoration:none;font-weight:500">{w['ward_name']}</a></td>
              <td style="color:#6B7280;font-size:11px">{w['zone_name']}</td>
              <td><span style="background:{c['bg']};color:{c['text']};padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600">{c['label']}</span></td>
              <td style="font-weight:700;color:{c['text']};font-family:monospace">{w['readiness_score']:.1f}</td>
              <td style="font-size:11px;color:#6B7280">{w.get('flood_type','—').title()}</td>
              <td style="font-size:11px;color:#6B7280">{w.get('trigger_rate_pct','—')}%</td>
            </tr>"""
        return rows

    all_rows = _rows(red, "red") + _rows(amber, "amber") + _rows(green, "green")

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<title>MCD Pre-Monsoon Readiness — All Wards</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  * {{ box-sizing:border-box;margin:0;padding:0; }}
  body {{ font-family:'Inter',sans-serif;background:#F9FAFB;color:#111827;padding:24px; }}
  @media print {{ body{{padding:0;background:white;}} .no-print{{display:none}} }}
  h1 {{ font-size:20px;font-weight:700;margin-bottom:4px; }}
  .meta {{ font-size:12px;color:#6B7280;margin-bottom:16px; }}
  .summary-pills {{ display:flex;gap:10px;margin-bottom:16px; }}
  .pill {{ padding:6px 14px;border-radius:20px;font-size:12px;font-weight:600; }}
  .pill.red   {{ background:#FEE2E2;color:#991B1B; }}
  .pill.amber {{ background:#FEF3C7;color:#92400E; }}
  .pill.green {{ background:#DCFCE7;color:#14532D; }}
  table {{ width:100%;border-collapse:collapse;font-size:13px; }}
  th {{ font-size:10px;font-weight:600;color:#6B7280;text-transform:uppercase;
        letter-spacing:.05em;padding:6px 10px;border-bottom:2px solid #E5E7EB;text-align:left; }}
  td {{ padding:7px 10px;border-bottom:1px solid #F3F4F6;vertical-align:middle; }}
  tr:hover td {{ background:#F9FAFB; }}
  .print-btn {{ position:fixed;bottom:24px;right:24px;padding:10px 20px;
                background:#2563EB;color:white;border:none;border-radius:8px;
                font-size:13px;font-weight:600;cursor:pointer; }}
</style>
</head><body>
<h1>MCD Pre-Monsoon Readiness — All Wards</h1>
<div class="meta">Generated: {now_str} · Urban Hydrology Engine · Click any ward name to view full report</div>
<div class="summary-pills">
  <span class="pill red">{len(red)} Critical</span>
  <span class="pill amber">{len(amber)} At Risk</span>
  <span class="pill green">{len(green)} Ready</span>
</div>
<table>
  <thead><tr><th>#</th><th>Ward</th><th>Zone</th><th>Status</th><th>Score</th><th>Flood Type</th><th>Trigger Rate</th></tr></thead>
  <tbody>{all_rows}</tbody>
</table>
<button class="print-btn no-print" onclick="window.print()">🖨 Print / Save PDF</button>
</body></html>""")
