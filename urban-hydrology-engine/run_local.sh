#!/bin/bash
# run_local.sh — Local dev runner (Mac / Linux)
# Loads backend/.env then starts uvicorn.
#
# Usage (from urban-hydrology-engine folder):
#   bash run_local.sh
#   chmod +x run_local.sh && ./run_local.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
ENV_FILE="$BACKEND_DIR/.env"

# Load .env if it exists
if [ -f "$ENV_FILE" ]; then
    echo "[run_local] Loading $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "[run_local] WARNING: $ENV_FILE not found"
    echo "            Copy backend/.env.example to backend/.env and set DATABASE_URL"
fi

# Validate DATABASE_URL
if [ -z "$DATABASE_URL" ]; then
    echo ""
    echo "ERROR: DATABASE_URL is not set."
    echo ""
    echo "Set it in backend/.env:"
    echo "  DATABASE_URL=postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres"
    echo ""
    echo "Get this from: Supabase Dashboard → Project Settings → Database → URI"
    exit 1
fi

MASKED=$(echo "$DATABASE_URL" | sed 's/:\/\/[^:]*:[^@]*@/:\/\/****:****@/')
echo "[run_local] DATABASE_URL = $MASKED"
echo "[run_local] Starting uvicorn on http://127.0.0.1:8000 ..."
echo "[run_local] Press Ctrl+C to stop"
echo ""

cd "$BACKEND_DIR"
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
