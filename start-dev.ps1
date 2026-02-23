# start-dev.ps1 - Launch backend (Poetry) + frontend (Vite) for development
$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$BackendPort = 5000
$FrontendPort = 5001

# -- Patch frontend config to point at the backend port -----------------------
$configFile = Join-Path $ProjectRoot "frontend\public\config.js"
(Get-Content $configFile -Raw) -replace 'API_URL:\s*"http://[^"]*"', "API_URL: `"http://127.0.0.1:$BackendPort/api/v1`"" |
    Set-Content $configFile -NoNewline

# -- Kill anything already on these ports -------------------------------------
foreach ($port in @($BackendPort, $FrontendPort)) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    if ($conn) {
        $procId = $conn.OwningProcess | Select-Object -First 1
        Write-Host "Killing existing process on port $port (PID $procId)..." -ForegroundColor Yellow
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 500
    }
}

# -- Start backend ------------------------------------------------------------
Write-Host "Starting backend on http://127.0.0.1:$BackendPort ..." -ForegroundColor Cyan
$backend = Start-Process -PassThru powershell -ArgumentList @(
    "-NoExit", "-Command",
    "Set-Location '$ProjectRoot'; poetry run python -m sql_databricks_bridge.server --host 127.0.0.1 --port $BackendPort"
)

# -- Start frontend -----------------------------------------------------------
Write-Host "Starting frontend on http://127.0.0.1:$FrontendPort ..." -ForegroundColor Cyan
$frontend = Start-Process -PassThru powershell -ArgumentList @(
    "-NoExit", "-Command",
    "Set-Location '$ProjectRoot\frontend'; npx vite --host 127.0.0.1 --port $FrontendPort"
)

# -- Wait for backend health check --------------------------------------------
Write-Host "Waiting for backend..." -ForegroundColor Gray -NoNewline
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    try {
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:$BackendPort/api/v1/health/live" -TimeoutSec 2
        if ($r.status -eq "ok") { $ready = $true; break }
    } catch {}
    Write-Host "." -NoNewline
}
Write-Host ""

if (-not $ready) {
    Write-Host "Backend failed to start!" -ForegroundColor Red
    Stop-Process -Id $backend.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $frontend.Id -Force -ErrorAction SilentlyContinue
    exit 1
}

Write-Host ""
Write-Host "=======================================" -ForegroundColor Green
Write-Host "  SQL-Databricks Bridge (dev mode)"     -ForegroundColor Green
Write-Host "  Backend:  http://127.0.0.1:$BackendPort"  -ForegroundColor White
Write-Host "  Frontend: http://127.0.0.1:$FrontendPort/sql-databricks-bridge/" -ForegroundColor White
Write-Host "  Press Enter to stop both."             -ForegroundColor Yellow
Write-Host "=======================================" -ForegroundColor Green
Write-Host ""

# -- Wait for user to press Enter, then clean up ------------------------------
Read-Host | Out-Null
Write-Host "Shutting down..." -ForegroundColor Yellow
Stop-Process -Id $backend.Id  -Force -ErrorAction SilentlyContinue
Stop-Process -Id $frontend.Id -Force -ErrorAction SilentlyContinue
Write-Host "Stopped." -ForegroundColor Green
