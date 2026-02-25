$jobId = "de4eb890-d284-4ec2-8f90-d192791eee70"
$poll = 0
do {
    $poll++
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/events/$jobId"
    $ts = (Get-Date).ToString("HH:mm:ss")
    Write-Host "[$ts] Poll #$poll | Status: $($r.status) | Step: $($r.current_step) | Queries: $($r.queries_completed)/$($r.queries_total) | Failed: $($r.queries_failed) | Rows: $($r.total_rows_extracted)"
    if ($r.status -eq "completed" -or $r.status -eq "failed") {
        Write-Host "--- FINAL RESULT ---"
        $r | ConvertTo-Json -Depth 10
        break
    }
    Start-Sleep -Seconds 30
} while ($true)
