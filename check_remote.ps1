try {
    $r = Invoke-WebRequest -Uri 'http://myworkspace.kantar.com:8000/api/v1/health' -UseBasicParsing -TimeoutSec 5
    Write-Host "Status: $($r.StatusCode)"
    Write-Host $r.Content
} catch {
    Write-Host "Error: $($_.Exception.Message)"
}
