# =============================================================================
# Extract PS_LATAM Tables from All KTCLSQL Servers
# =============================================================================
# Usage:
#   .\extract_ps_latam_all_servers.ps1          # Extract from all servers
#   .\extract_ps_latam_all_servers.ps1 -Server 2    # Extract from KTCLSQL002 only
# =============================================================================

param(
    [Parameter(Mandatory=$false)]
    [int]$Server = 0  # 0 = all servers, 1-4 = specific server
)

$ErrorActionPreference = "Continue"

# Configuration
$CATALOG = "000-sql-databricks-bridge"
$QUERIES_BASE = "queries/servers"
$DATABASE = "PS_LATAM"

# Server list
if ($Server -gt 0) {
    $SERVERS = @($Server)
    Write-Host "Single server mode: KTCLSQL00$Server" -ForegroundColor Yellow
} else {
    $SERVERS = @(1, 2, 3, 4)
    Write-Host "Multi-server mode: KTCLSQL001-004" -ForegroundColor Green
}

# Tables to extract
$TABLES = @("loc_psdata_compras", "loc_psdata_procesado")

# Function to extract a single table
function Extract-Table {
    param(
        [int]$ServerNum,
        [string]$TableName
    )
    
    $ServerName = "KTCLSQL00$ServerNum.KT.group.local"
    $Schema = "KTCLSQL00$ServerNum"
    $QueriesPath = "$QUERIES_BASE/KTCLSQL00$ServerNum"
    
    Write-Host ""
    Write-Host "========================================================================"
    Write-Host "Extracting: $TableName" -ForegroundColor Cyan
    Write-Host "  Server:   $ServerName"
    Write-Host "  Database: $DATABASE"
    Write-Host "  Target:   $CATALOG.$Schema.$TableName"
    Write-Host "========================================================================"
    
    # Check if query file exists
    $QueryFile = "$QueriesPath/$TableName.sql"
    if (-not (Test-Path $QueryFile)) {
        Write-Host "ERROR: Query file not found: $QueryFile" -ForegroundColor Red
        return $false
    }
    
    # Run extraction
    try {
        poetry run sql-databricks-bridge extract `
            --queries-path "$QueriesPath" `
            --server "$ServerName" `
            --database "$DATABASE" `
            --query-name "$TableName" `
            --destination "$CATALOG.$Schema" `
            --verbose
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "SUCCESS: $TableName from $ServerName" -ForegroundColor Green
            return $true
        } else {
            Write-Host "FAILED: $TableName from $ServerName (Exit code: $LASTEXITCODE)" -ForegroundColor Red
            return $false
        }
    } catch {
        Write-Host "FAILED: $TableName from $ServerName" -ForegroundColor Red
        Write-Host "Error: $_" -ForegroundColor Red
        return $false
    }
}

# Main extraction loop
Write-Host ""
Write-Host "========================================================================"
Write-Host "       Starting PS_LATAM Multi-Server Extraction" -ForegroundColor Green
Write-Host "========================================================================"
Write-Host ""

$TOTAL_SUCCESS = 0
$TOTAL_FAILED = 0

foreach ($ServerNum in $SERVERS) {
    Write-Host ""
    Write-Host "====================================================================="
    Write-Host "   Server: KTCLSQL00$ServerNum.KT.group.local" -ForegroundColor Yellow
    Write-Host "====================================================================="
    
    foreach ($Table in $TABLES) {
        if (Extract-Table -ServerNum $ServerNum -TableName $Table) {
            $TOTAL_SUCCESS++
        } else {
            $TOTAL_FAILED++
            Write-Host "Continuing with next table..." -ForegroundColor Yellow
        }
    }
}

# Summary
Write-Host ""
Write-Host "========================================================================"
Write-Host "                    EXTRACTION SUMMARY" -ForegroundColor Cyan
Write-Host "========================================================================"
Write-Host "  Successful extractions: $TOTAL_SUCCESS" -ForegroundColor Green
Write-Host "  Failed extractions:     $TOTAL_FAILED" -ForegroundColor Red
Write-Host ""

if ($TOTAL_FAILED -eq 0) {
    Write-Host "All extractions completed successfully!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "Some extractions failed. Check logs for details." -ForegroundColor Yellow
    exit 1
}
