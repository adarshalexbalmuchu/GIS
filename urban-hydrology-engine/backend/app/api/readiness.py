"""
Pre-Monsoon Readiness Score API routes.

GET /readiness/scores      — full ward list with scores, bands, flood types
GET /readiness/summary     — aggregate stats + countdown
GET /readiness/ward/{id}   — single ward detail with contributing factors
"""

from fastapi import APIRouter
from sqlalchemy import text

from app.db import engine
from app.services.readiness import compute_readiness_scores, readiness_summary

router = APIRouter(prefix="/readiness", tags=["readiness"])

# Module-level cache — recomputed on each explicit call, cached for 5 min
import time
_cache: dict = {"scores": None, "ts": 0.0}
_CACHE_TTL = 300  # seconds


async def _get_scores(force: bool = False) -> list[dict]:
    now = time.monotonic()
    if not force and _cache["scores"] and (now - _cache["ts"]) < _CACHE_TTL:
        return _cache["scores"]
    async with engine.connect() as conn:
        scores = await compute_readiness_scores(conn)
    _cache["scores"] = scores
    _cache["ts"] = now
    return scores


@router.get("/scores")
async def get_readiness_scores():
    """All 290 wards ranked by Pre-Monsoon Readiness Score."""
    scores = await _get_scores()
    summary = await readiness_summary(scores)
    return {"summary": summary, "wards": scores}


@router.get("/summary")
async def get_readiness_summary():
    """Quick aggregate: counts by band, avg score, days to monsoon."""
    scores = await _get_scores()
    return await readiness_summary(scores)


@router.get("/ward/{ward_id}")
async def get_ward_readiness(ward_id: int):
    """Single ward readiness detail with contributing factors."""
    scores = await _get_scores()
    match = next((s for s in scores if s["ward_id"] == ward_id), None)
    if not match:
        return {"error": f"Ward {ward_id} not found"}
    return match


@router.post("/refresh")
async def refresh_readiness():
    """Force recompute (bypass cache). Call after seeding new data."""
    scores = await _get_scores(force=True)
    summary = await readiness_summary(scores)
    return {"refreshed": True, "summary": summary}
