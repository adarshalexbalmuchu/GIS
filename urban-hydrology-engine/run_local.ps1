# run_local.ps1 — Local dev runner for Urban Hydrology Engine
# Loads .env, sets DATABASE_URL from it, then starts uvicorn.
#
# Usage (from urban-hydrology-engine folder):
#   .\run_local.ps1
#
# Prerequisites:
#   1. Create backend\.env from backend\.env.example
#   2. Set DATABASE_URL to your Supabase connection string in that file
#   3. pip install -r backend\requirements.txt (or use your venv)

$ErrorActionPreference = "Stop"

# ── Resolve paths ─────────────────────────────────────────────────────────
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$BackendDir = Join-Path $ScriptDir "backend"
$EnvFile    = Join-Path $BackendDir ".env"

if (-not (Test-Path $BackendDir)) {
    Write-Error "Backend directory not found: $BackendDir"
    exit 1
}

# Load .env file
if (Test-Path $EnvFile) {
    Write-Host "[run_local] Loading environment from $EnvFile" -ForegroundColor Cyan
    Get-Content $EnvFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line -split "=", 2
            if ($parts.Length -eq 2) {
                $key = $parts[0].Trim()
                $value = $parts[1].Trim().Trim('"').Trim("'")
                [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
            }
        }
    }
}
else {
    Write-Warning "[run_local] No .env found at $EnvFile"
    Write-Warning "            Copy backend\.env.example to backend\.env and fill in your Supabase URL."
    Write-Warning "            Continuing - DATABASE_URL must already be set in your shell environment."
}

# ── Validate DATABASE_URL ─────────────────────────────────────────────────
$dbUrl = [System.Environment]::GetEnvironmentVariable("DATABASE_URL", "Process")
if (-not $dbUrl) {
        Write-Error @"
DATABASE_URL is not set.

Set it in backend\.env:
    DATABASE_URL=postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres

Get this from: Supabase Dashboard -> Project Settings -> Database -> URI
"@
    exit 1
}

# Mask password in log output
$maskedUrl = $dbUrl -replace ":([^@]+)@", ":****@"
Write-Host "[run_local] DATABASE_URL = $maskedUrl" -ForegroundColor Green

# Default FRONTEND_DIR for local runs when not explicitly set
$frontendDir = Join-Path $ScriptDir "frontend"
$frontendDirEnv = [System.Environment]::GetEnvironmentVariable("FRONTEND_DIR", "Process")
if (-not $frontendDirEnv -and (Test-Path $frontendDir)) {
    [System.Environment]::SetEnvironmentVariable("FRONTEND_DIR", $frontendDir, "Process")
    Write-Host "[run_local] FRONTEND_DIR = $frontendDir" -ForegroundColor Green
}

# ── Start uvicorn from backend directory ─────────────────────────────────
Write-Host "[run_local] Starting uvicorn on http://127.0.0.1:8000 ..." -ForegroundColor Cyan
Write-Host "[run_local] Press Ctrl+C to stop" -ForegroundColor DarkGray
Write-Host ""

Set-Location $BackendDir
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
