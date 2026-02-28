"""
Database engine, session, and ORM table definitions for Urban Hydrology Engine.

Rules:
- SRID is always 4326 for all geometry storage
- All geometry columns have GIST spatial indexes
- Uses asyncpg as the async DB driver
"""

import os
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    func,
)
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker
from geoalchemy2 import Geometry


# ---------------------------------------------------------------------------
# Engine / Session
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://hydro:hydro123@localhost:5432/hydrology",
)

# asyncpg requires the postgresql+asyncpg:// scheme.
# Render provides postgres:// URLs; handle both prefixes.
import re
ASYNC_DATABASE_URL = re.sub(
    r"^postgres(ql)?://", "postgresql+asyncpg://", DATABASE_URL
)

engine = create_async_engine(ASYNC_DATABASE_URL, echo=False, future=True)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

class Ward(Base):
    __tablename__ = "wards"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    ward_no = Column(String, nullable=True)
    zone_name = Column(String, nullable=True)
    geom = Column(Geometry(geometry_type="GEOMETRY", srid=4326), nullable=False)

    hotspots = relationship("Hotspot", back_populates="ward")
    dispatch_runs = relationship("DispatchRun", back_populates="ward")


class Hotspot(Base):
    __tablename__ = "hotspots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ward_id = Column(Integer, ForeignKey("wards.id"), nullable=False)
    geom = Column(Geometry(geometry_type="POLYGON", srid=4326), nullable=False)
    capacity_c = Column(Float, nullable=False, default=100.0)
    runoff_t = Column(Float, nullable=False, default=1.0)
    priority_weight = Column(Float, nullable=False, default=1.0)
    critical_penalty_pc = Column(Float, nullable=False, default=0.0)
    zone_name = Column(String, nullable=True)

    ward = relationship("Ward", back_populates="hotspots")
    sensor_events = relationship("SensorEvent", back_populates="hotspot")

    __table_args__ = (
        Index("idx_hotspots_ward_id", ward_id),
    )


class RainEvent(Base):
    __tablename__ = "rain_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    geom = Column(Geometry(geometry_type="POLYGON", srid=4326), nullable=False)
    intensity_r = Column(Float, nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())

    __table_args__ = (
        Index("idx_rain_events_created_at", created_at),
    )


class SensorEvent(Base):
    __tablename__ = "sensor_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hotspot_id = Column(Integer, ForeignKey("hotspots.id"), nullable=False)
    delta_capacity = Column(Float, nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())

    hotspot = relationship("Hotspot", back_populates="sensor_events")

    __table_args__ = (
        Index("idx_sensor_events_hotspot_id", hotspot_id),
        Index("idx_sensor_events_created_at", created_at),
    )


class DispatchRun(Base):
    __tablename__ = "dispatch_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ward_id = Column(Integer, ForeignKey("wards.id"), nullable=False)
    ws_score = Column(Float, nullable=False)
    status = Column(String, nullable=False)
    result_json = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())

    ward = relationship("Ward", back_populates="dispatch_runs")

    __table_args__ = (
        Index("idx_dispatch_runs_ward_id", ward_id),
        Index("idx_dispatch_runs_created_at", created_at),
    )


class CriticalInfrastructure(Base):
    __tablename__ = "critical_infrastructure"

    id = Column(Integer, primary_key=True, autoincrement=True)
    osm_id = Column(BigInteger, nullable=True)
    facility_type = Column(String, nullable=False)
    name = Column(String, nullable=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    geom = Column(Geometry(geometry_type="POINT", srid=4326), nullable=False)

    __table_args__ = (
        Index("idx_critical_infra_type", facility_type),
    )


class WardElevation(Base):
    __tablename__ = "ward_elevation"

    ward_id = Column(Integer, ForeignKey("wards.id"), primary_key=True)
    mean_elevation = Column(Float)
    min_elevation = Column(Float)
    max_elevation = Column(Float)
    elevation_range = Column(Float)
    mean_slope = Column(Float)
    max_slope = Column(Float)
    runoff_t = Column(Float)
    terrain_class = Column(String)


# ---------------------------------------------------------------------------
# init_db — creates all tables (no Alembic)
# ---------------------------------------------------------------------------

async def init_db() -> None:
    """Create PostGIS extension and all ORM tables (idempotent)."""
    from sqlalchemy import text as sa_text

    async with engine.begin() as conn:
        # Ensure PostGIS is available
        await conn.execute(sa_text("CREATE EXTENSION IF NOT EXISTS postgis"))

    # Separate transaction for table creation so PostGIS types are visible
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
