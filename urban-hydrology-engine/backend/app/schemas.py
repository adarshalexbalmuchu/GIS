"""
Pydantic v2 schemas for the Urban Hydrology Engine API.
"""

from pydantic import BaseModel, field_validator


# ---------------------------------------------------------------------------
# Rain ingestion
# ---------------------------------------------------------------------------

class RainIngestRequest(BaseModel):
    geojson_polygon: dict
    intensity_r: float

    @field_validator("intensity_r")
    @classmethod
    def clamp_intensity(cls, v: float) -> float:
        """Intensity must be at least 0.1."""
        return max(v, 0.1)


class RainIngestResponse(BaseModel):
    event_id: int
    hotspots_in_polygon: int
    message: str


# ---------------------------------------------------------------------------
# Sensor ingestion
# ---------------------------------------------------------------------------

class SensorIngestRequest(BaseModel):
    hotspot_id: int
    delta_capacity: float


class SensorIngestResponse(BaseModel):
    event_id: int
    hotspot_id: int
    new_capacity: float
    message: str
