$src = "C:\Users\70088287\Documents\Projects\sql-databricks-bridge\queries"
$dest = "\\KTCLFS001\Procesos\Data_PRQT_MKA\sql-databricks-bridge\queries"

# Remove existing destination if it exists
if (Test-Path $dest) {
    Remove-Item -Path $dest -Recurse -Force
    Write-Host "Removed old $dest"
}

# Copy entire queries directory
Copy-Item -Path $src -Destination $dest -Recurse -Force
Write-Host "Copied queries to $dest"

# Verify
$count = (Get-ChildItem -Path "$dest\countries" -Directory).Count
Write-Host "Countries found: $count"
Get-ChildItem -Path "$dest\countries" -Directory | Select-Object Name
