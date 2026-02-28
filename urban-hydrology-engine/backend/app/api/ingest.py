"""
Ingest API routes — store rain and sensor events (no scoring).
"""

import json

from fastapi import APIRouter, Depends, HTTPException
from shapely.geometry import shape
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.auth import verify_api_key
from app.db import async_session, engine
from app.schemas import (
    RainIngestRequest,
    RainIngestResponse,
    SensorIngestRequest,
    SensorIngestResponse,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Internal helpers (callable by both HTTP handlers and the scheduler)
# ---------------------------------------------------------------------------

async def ingest_rain_internal(event: dict, conn: AsyncConnection) -> dict:
    """
    Insert a rain event and count intersecting hotspots.
    `event` must have keys: geojson_polygon, intensity_r.
    Returns {"event_id": int, "hotspots_in_polygon": int}.
    """
    wkt = shape(event["geojson_polygon"]).wkt
    geom_str = f"SRID=4326;{wkt}"

    row = await conn.execute(
        text(
            "INSERT INTO rain_events (geom, intensity_r, created_at) "
            "VALUES (ST_GeomFromEWKT(:geom), :intensity, now()) "
            "RETURNING id"
        ),
        {"geom": geom_str, "intensity": event["intensity_r"]},
    )
    event_id = row.scalar_one()

    count_row = await conn.execute(
        text(
            "SELECT COUNT(*) FROM hotspots "
            "WHERE ST_Intersects(geom, ST_GeomFromEWKT(:geom))"
        ),
        {"geom": geom_str},
    )
    hotspot_count = count_row.scalar_one()

    return {"event_id": event_id, "hotspots_in_polygon": hotspot_count}


# ---------------------------------------------------------------------------
# POST /ingest/rain
# ---------------------------------------------------------------------------

@router.post("/ingest/rain", response_model=RainIngestResponse)
async def ingest_rain(req: RainIngestRequest, _auth=Depends(verify_api_key)):
    """Store a rainfall polygon and count intersecting hotspots."""
    async with engine.begin() as conn:
        result = await ingest_rain_internal(
            {"geojson_polygon": req.geojson_polygon, "intensity_r": req.intensity_r},
            conn,
        )

    return RainIngestResponse(
        event_id=result["event_id"],
        hotspots_in_polygon=result["hotspots_in_polygon"],
        message=f"Rain event stored. {result['hotspots_in_polygon']} hotspots in polygon.",
    )


# ---------------------------------------------------------------------------
# POST /ingest/sensor
# ---------------------------------------------------------------------------

@router.post("/ingest/sensor", response_model=SensorIngestResponse)
async def ingest_sensor(req: SensorIngestRequest):
    """Store a sensor reading and update hotspot capacity."""
    async with async_session() as session:
        # Check hotspot exists
        exists = await session.execute(
            text("SELECT id FROM hotspots WHERE id = :hid"),
            {"hid": req.hotspot_id},
        )
        if exists.scalar_one_or_none() is None:
            raise HTTPException(status_code=404, detail="Hotspot not found")

        # Insert sensor event
        row = await session.execute(
            text(
                "INSERT INTO sensor_events (hotspot_id, delta_capacity, created_at) "
                "VALUES (:hid, :delta, now()) RETURNING id"
            ),
            {"hid": req.hotspot_id, "delta": req.delta_capacity},
        )
        event_id = row.scalar_one()

        # Update capacity clamped to [0, 100]
        cap_row = await session.execute(
            text(
                "UPDATE hotspots "
                "SET capacity_c = GREATEST(0, LEAST(100, capacity_c + :delta)) "
                "WHERE id = :hid RETURNING capacity_c"
            ),
            {"delta": req.delta_capacity, "hid": req.hotspot_id},
        )
        new_cap = cap_row.scalar_one()

        await session.commit()

    return SensorIngestResponse(
        event_id=event_id,
        hotspot_id=req.hotspot_id,
        new_capacity=round(new_cap, 2),
        message=f"Capacity updated to {round(new_cap, 2)}",
    )
