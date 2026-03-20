"""
Backtest API routes.

POST /backtest/2023        — enqueue background replay (returns job_id instantly)
GET  /backtest/2023/status — poll job status: queued | running | complete | error
GET  /backtest/2023/result — return cached result when complete
"""

import asyncio
import time
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, Depends

from app.auth import verify_api_key
from app.db import engine
from app.services.backtest import run_backtest_2023
from app.ws import manager as ws_manager

router = APIRouter(prefix="/backtest", tags=["backtest"])

# ── Module-level job state ────────────────────────────────────────────────
_job: dict = {
    "status":      "idle",      # idle | running | complete | error
    "started_at":  None,
    "finished_at": None,
    "progress":    [],          # list of progress messages streamed to UI
    "result":      None,
    "error":       None,
}


def _reset_job():
    _job["status"]      = "running"
    _job["started_at"]  = datetime.utcnow().isoformat()
    _job["finished_at"] = None
    _job["progress"]    = []
    _job["result"]      = None
    _job["error"]       = None


async def _run_job():
    """Background coroutine — runs backtest, updates _job state, broadcasts via WS."""
    try:
        await ws_manager.broadcast({"type": "backtest_progress",
                                    "msg": "Starting 2023 replay…", "pct": 0})
        _job["progress"].append("Starting 2023 replay…")

        async with engine.connect() as conn:
            result = await run_backtest_2023(conn, progress_cb=_progress_cb)

        _job["result"]      = result
        _job["status"]      = "complete"
        _job["finished_at"] = datetime.utcnow().isoformat()

        await ws_manager.broadcast({
            "type":      "backtest_complete",
            "precision": result["precision"],
            "recall":    result["recall"],
            "f1_score":  result["f1_score"],
        })

    except Exception as exc:
        _job["status"]      = "error"
        _job["error"]       = str(exc)
        _job["finished_at"] = datetime.utcnow().isoformat()
        await ws_manager.broadcast({"type": "backtest_error", "msg": str(exc)})


async def _progress_cb(msg: str, pct: int):
    """Called by run_backtest_2023 after each event to push progress."""
    _job["progress"].append(msg)
    await ws_manager.broadcast({"type": "backtest_progress", "msg": msg, "pct": pct})


# ── Routes ────────────────────────────────────────────────────────────────

@router.post("/2023")
async def trigger_backtest_2023(
    background_tasks: BackgroundTasks,
    _=Depends(verify_api_key),
):
    """
    Enqueue the 2023 replay as a background task.
    Returns immediately — poll GET /backtest/2023/status for progress.
    """
    if _job["status"] == "running":
        return {"status": "running", "msg": "Replay already in progress."}

    _reset_job()
    background_tasks.add_task(_run_job)

    return {
        "status":  "queued",
        "job_id":  "backtest-2023",
        "msg":     "2023 replay enqueued. Poll /backtest/2023/status.",
    }


@router.get("/2023/status")
async def backtest_status():
    """Poll job status. Returns: status, progress log, elapsed seconds."""
    elapsed = None
    if _job["started_at"]:
        start = datetime.fromisoformat(_job["started_at"])
        end   = (datetime.fromisoformat(_job["finished_at"])
                 if _job["finished_at"] else datetime.utcnow())
        elapsed = round((end - start).total_seconds(), 1)

    return {
        "status":      _job["status"],
        "started_at":  _job["started_at"],
        "finished_at": _job["finished_at"],
        "elapsed_s":   elapsed,
        "progress":    _job["progress"][-10:],   # last 10 messages
        "error":       _job["error"],
    }


@router.get("/2023/result")
async def get_backtest_result():
    """Return the cached result. Only available when status == complete."""
    if _job["status"] == "running":
        return {"error": "Replay still running. Poll /backtest/2023/status."}
    if _job["status"] == "error":
        return {"error": f"Replay failed: {_job['error']}"}
    if not _job["result"]:
        return {"error": "No result yet. POST /backtest/2023 to start."}
    return _job["result"]
