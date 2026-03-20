"""
2023 Delhi Flood Backtest Engine.

Replays the July–August 2023 monsoon season — Delhi's worst flood in 45 years.
Peak: Yamuna reached 208.66m on July 13 2023 (highest since 1978).
      153mm fell in a single day. 27,000+ families displaced.

What this does:
  1. Generates synthetic but historically-grounded rain events for key 2023 dates
  2. Scores all wards using the existing PMRS engine against those events
  3. Returns per-ward predicted status alongside known-flooded ward references
  4. Computes precision/recall vs a reference set of historically-flooded wards

The reference flooded-ward list is derived from:
  - NRSC 2023 flood inundation satellite data
  - Known low-elevation areas along Yamuna (< 207m)
  - Reported flooding in Delhi news: Yamuna Khadar, Civil Lines, 
    Kashmere Gate, ITO area, Mayur Vihar, Wazirabad, Burari, 
    Usmanpur, Gokulpuri, Mustafabad, Shahdara, Geeta Colony
"""

from datetime import datetime, timedelta
import random
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

# ---------------------------------------------------------------------------
# 2023 KEY EVENTS — historically documented
# ---------------------------------------------------------------------------

EVENTS_2023 = [
    {
        "date": "2023-07-09",
        "rain_mm": 41.0,
        "label": "Pre-peak heavy rain",
        "yamuna_level_m": 204.8,
        "intensity_r": 4.5,
    },
    {
        "date": "2023-07-10",
        "rain_mm": 63.0,
        "label": "Yamuna crosses danger level",
        "yamuna_level_m": 205.6,
        "intensity_r": 6.2,
    },
    {
        "date": "2023-07-11",
        "rain_mm": 153.0,
        "label": "PEAK — 153mm single day, Yamuna 207.7m",
        "yamuna_level_m": 207.7,
        "intensity_r": 9.8,
        "peak": True,
    },
    {
        "date": "2023-07-12",
        "rain_mm": 88.0,
        "label": "Yamuna peaks at 208.66m — highest since 1978",
        "yamuna_level_m": 208.66,
        "intensity_r": 8.5,
        "peak": True,
    },
    {
        "date": "2023-07-13",
        "rain_mm": 44.0,
        "label": "Sustained — floodplain inundated",
        "yamuna_level_m": 207.9,
        "intensity_r": 5.0,
    },
    {
        "date": "2023-07-14",
        "rain_mm": 22.0,
        "label": "Recession begins",
        "yamuna_level_m": 206.8,
        "intensity_r": 3.0,
    },
]

# ---------------------------------------------------------------------------
# Reference flooded wards (ground truth from NRSC 2023 + news reports)
# These ward names are partial matches — we do case-insensitive substring match
# ---------------------------------------------------------------------------

KNOWN_FLOODED_WARD_FRAGMENTS = [
    # Yamuna floodplain / river flood
    "yamuna", "khadar", "wazirabad", "burari", "usmanpur",
    "gokulpuri", "mustafabad", "karawal", "bhajanpura",
    # Civil Lines / ISBT area
    "civil lines", "kashmere", "sadar bazar", "chandni chowk",
    # East Delhi / Trans-Yamuna
    "geeta colony", "krishna nagar", "gandhi nagar", "vishwas nagar",
    "shahdara", "mayur vihar", "patparganj", "kalyanpuri",
    # ITO / Central
    "ito", "rajghat", "mori gate",
    # Najafgarh / West (backflow)
    "najafgarh", "dwarka", "matiala", "uttam nagar",
    # North Delhi drains
    "mukherjee nagar", "jahangirpuri", "rohini",
]


def _is_reference_flooded(ward_name: str) -> bool:
    name_lower = ward_name.lower()
    return any(f in name_lower for f in KNOWN_FLOODED_WARD_FRAGMENTS)


# ---------------------------------------------------------------------------
# Simulation geometry helpers
# ---------------------------------------------------------------------------

def _full_delhi_polygon():
    """Bounding polygon covering all of Delhi (EPSG:4326)."""
    return {
        "type": "Polygon",
        "coordinates": [[[76.83, 28.40], [77.55, 28.40],
                          [77.55, 28.90], [76.83, 28.90],
                          [76.83, 28.40]]]
    }


def _yamuna_corridor_polygon():
    """Narrow polygon covering the Yamuna floodplain corridor."""
    return {
        "type": "Polygon",
        "coordinates": [[[77.20, 28.52], [77.32, 28.52],
                          [77.32, 28.82], [77.20, 28.82],
                          [77.20, 28.52]]]
    }


# ---------------------------------------------------------------------------
# Main backtest function
# ---------------------------------------------------------------------------

async def run_backtest_2023(conn: AsyncConnection, progress_cb=None) -> dict:
    """
    Run the 2023 backtest:
      - Insert synthetic rain events for each key 2023 date
      - Score all wards for EACH event date
      - Compute aggregate predicted status per ward
      - Compare against reference flooded wards
      - Return results + precision/recall
    """
    from app.services.scoring import compute_ward_score
    from app.api.ingest import ingest_rain_internal  # reuse existing logic

    # ── Fetch all wards ───────────────────────────────────────────────────
    ward_rows = await conn.execute(text(
        "SELECT id, name FROM wards ORDER BY id"
    ))
    wards = ward_rows.mappings().fetchall()
    if not wards:
        return {"error": "No wards found. Run import_delhi_wards.py first."}

    # ── Clear any existing backtest rain events (tagged with source) ──────
    await conn.execute(text(
        "DELETE FROM rain_events WHERE created_at > NOW() - INTERVAL '30 days' "
        "AND intensity_r >= 9.0"  # proxy tag: peak events only
    ))
    await conn.commit()

    # ── For each event, insert rain + score all wards ─────────────────────
    ward_triggered_count = {w["id"]: 0 for w in wards}
    ward_critical_count  = {w["id"]: 0 for w in wards}
    ward_worst_score     = {w["id"]: 100.0 for w in wards}

    event_summaries = []

    for event in EVENTS_2023:
        # Insert rain event covering Delhi
        poly = _yamuna_corridor_polygon() if event["yamuna_level_m"] > 206.5 \
               else _full_delhi_polygon()

        result = await ingest_rain_internal(
            {"geojson_polygon": poly, "intensity_r": event["intensity_r"]},
            conn
        )
        await conn.commit()

        # Score all wards against this rain event
        n_triggered = 0
        n_critical  = 0
        for ward in wards:
            score = await compute_ward_score(ward["id"], conn)
            if score["triggered"]:
                n_triggered += 1
                ward_triggered_count[ward["id"]] += 1
            if score["ws_score"] < 0:
                n_critical += 1
                ward_critical_count[ward["id"]] += 1
            if score["ws_score"] < ward_worst_score[ward["id"]]:
                ward_worst_score[ward["id"]] = score["ws_score"]

        event_summaries.append({
            "date":          event["date"],
            "label":         event["label"],
            "rain_mm":       event["rain_mm"],
            "yamuna_m":      event["yamuna_level_m"],
            "intensity_r":   event["intensity_r"],
            "wards_triggered": n_triggered,
            "wards_critical":  n_critical,
            "peak":          event.get("peak", False),
        })

        if progress_cb:
            pct = int((len(event_summaries) / len(EVENTS_2023)) * 80)
            await progress_cb(
                f"{event['date']} scored — {n_triggered} wards triggered, "
                f"{n_critical} critical",
                pct
            )

        # Remove rain events after each day so they don't stack
        await conn.execute(text(
            "DELETE FROM rain_events WHERE created_at > NOW() - INTERVAL '5 minutes'"
        ))
        await conn.commit()

    # ── Build per-ward result ─────────────────────────────────────────────
    ward_results = []
    true_positives  = 0
    false_positives = 0
    false_negatives = 0
    true_negatives  = 0

    for ward in wards:
        wid   = ward["id"]
        wname = ward["name"]

        triggered_days = ward_triggered_count[wid]
        critical_days  = ward_critical_count[wid]
        worst_score    = round(ward_worst_score[wid], 1)
        n_events       = len(EVENTS_2023)

        # Model prediction: triggered on ≥ 2 out of 6 event days = predicted flooded
        predicted_flooded = triggered_days >= 2
        reference_flooded = _is_reference_flooded(wname)

        if predicted_flooded and reference_flooded:
            match = "true_positive"
            true_positives += 1
        elif predicted_flooded and not reference_flooded:
            match = "false_positive"
            false_positives += 1
        elif not predicted_flooded and reference_flooded:
            match = "false_negative"
            false_negatives += 1
        else:
            match = "true_negative"
            true_negatives += 1

        ward_results.append({
            "ward_id":          wid,
            "ward_name":        wname,
            "triggered_days":   triggered_days,
            "critical_days":    critical_days,
            "worst_score":      worst_score,
            "predicted_flooded": predicted_flooded,
            "reference_flooded": reference_flooded,
            "match":            match,
            "trigger_rate_pct": round(triggered_days / n_events * 100, 1),
        })

    # Sort: worst match / highest risk first
    match_order = {"false_negative": 0, "true_positive": 1,
                   "false_positive": 2, "true_negative": 3}
    ward_results.sort(key=lambda r: (
        match_order[r["match"]],
        -r["trigger_rate_pct"]
    ))

    # ── Precision / Recall ────────────────────────────────────────────────
    precision = (true_positives / (true_positives + false_positives)
                 if (true_positives + false_positives) > 0 else 0.0)
    recall    = (true_positives / (true_positives + false_negatives)
                 if (true_positives + false_negatives) > 0 else 0.0)
    f1        = (2 * precision * recall / (precision + recall)
                 if (precision + recall) > 0 else 0.0)

    n_ref_flooded    = sum(1 for r in ward_results if r["reference_flooded"])
    n_pred_flooded   = sum(1 for r in ward_results if r["predicted_flooded"])

    return {
        "backtest_year":    2023,
        "peak_event":       "July 12 2023 — Yamuna 208.66m (highest since 1978)",
        "total_wards":      len(ward_results),
        "n_reference_flooded": n_ref_flooded,
        "n_predicted_flooded": n_pred_flooded,
        "true_positives":   true_positives,
        "false_positives":  false_positives,
        "false_negatives":  false_negatives,
        "true_negatives":   true_negatives,
        "precision":        round(precision * 100, 1),
        "recall":           round(recall    * 100, 1),
        "f1_score":         round(f1        * 100, 1),
        "events":           event_summaries,
        "wards":            ward_results,
        "computed_at":      datetime.utcnow().isoformat(),
    }
