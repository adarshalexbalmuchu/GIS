"""API key authentication dependency."""

import os

from fastapi import Header, HTTPException

API_SECRET_KEY = os.getenv("API_SECRET_KEY", "hydro-mvp-secret-2026")


async def verify_api_key(x_api_key: str = Header(...)):
    """Validate X-API-Key header against the configured secret."""
    if x_api_key != API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True
