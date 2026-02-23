$urls = @(
    'http://myworkspace.kantar.com:8000/',
    'http://myworkspace.kantar.com:8000/api/v1/health',
    'http://myworkspace.kantar.com:8000/api/v1/metadata/stages',
    'http://myworkspace.kantar.com:8000/health',
    'http://myworkspace.kantar.com:8000/docs'
)
foreach ($url in $urls) {
    try {
        $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
        Write-Host "$($r.StatusCode) $url"
    } catch {
        $code = $_.Exception.Response.StatusCode.value__
        Write-Host "$code $url"
    }
}
