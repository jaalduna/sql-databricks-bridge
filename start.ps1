# SQL-Databricks Bridge - Launch Script
# Starts the backend (compiled exe) and the desktop app (Tauri)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

$BackendExe = Join-Path $ProjectRoot "dist-test\sql-databricks-bridge.exe"
$DesktopApp = "$env:LOCALAPPDATA\SQL Databricks Bridge\sql-databricks-bridge.exe"

# -- Validate paths ----------------------------------------------------------
if (-not (Test-Path $BackendExe)) {
    Write-Host "Backend exe not found: $BackendExe" -ForegroundColor Red
    exit 1
}
if (-not (Test-Path $DesktopApp)) {
    Write-Host "Desktop app not found: $DesktopApp" -ForegroundColor Red
    Write-Host "Install it from the latest GitHub release (SQL.Databricks.Bridge_*_x64-setup.exe)" -ForegroundColor Yellow
    exit 1
}

# -- Kill any existing backend on port 8000 -----------------------------------
$conn = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
if ($conn) {
    $procId = $conn.OwningProcess | Select-Object -First 1
    Write-Host "Killing existing process on port 8000 (PID $procId)..." -ForegroundColor Yellow
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}

# -- Start backend ------------------------------------------------------------
Write-Host "Starting backend on http://127.0.0.1:8000 ..." -ForegroundColor Cyan
$backend = Start-Process -FilePath $BackendExe `
    -ArgumentList "serve", "--host", "127.0.0.1", "--port", "8000" `
    -WorkingDirectory $ProjectRoot `
    -PassThru -NoNewWindow

# -- Wait for backend to be ready ---------------------------------------------
Write-Host "Waiting for backend..." -ForegroundColor Gray -NoNewline
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    try {
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/health/live" -TimeoutSec 2
        if ($r.status -eq "ok") { $ready = $true; break }
    } catch {}
    Write-Host "." -NoNewline
}
Write-Host ""
if (-not $ready) {
    Write-Host "Backend failed to start!" -ForegroundColor Red
    Stop-Process -Id $backend.Id -Force -ErrorAction SilentlyContinue
    exit 1
}
Write-Host "Backend ready (PID $($backend.Id))" -ForegroundColor Green

# -- Launch desktop app -------------------------------------------------------
Write-Host "Launching desktop app..." -ForegroundColor Cyan
$desktop = Start-Process -FilePath $DesktopApp -PassThru

Write-Host ""
Write-Host "=======================================" -ForegroundColor Green
Write-Host "  SQL-Databricks Bridge is running"      -ForegroundColor Green
Write-Host "  Backend:  http://127.0.0.1:8000"       -ForegroundColor White
Write-Host "  Desktop:  SQL Databricks Bridge app"    -ForegroundColor White
Write-Host "  Close the desktop app to stop"          -ForegroundColor Gray
Write-Host "=======================================" -ForegroundColor Green
Write-Host ""

# -- Wait for desktop app to close, then stop backend -------------------------
try {
    Wait-Process -Id $desktop.Id
} catch {}
Write-Host "Desktop app closed. Shutting down backend..." -ForegroundColor Yellow
Stop-Process -Id $backend.Id -Force -ErrorAction SilentlyContinue
Write-Host "Stopped." -ForegroundColor Green
