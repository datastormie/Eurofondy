# Downloads the latest eufunds.duckdb from the "data-store" GitHub Release
# so the local copy in data/ stays in sync with what the monthly workflow published.
# No `gh` CLI dependency: the repo and release are public, so a plain REST call works.

$repo = "datastormie/Eurofondy"
$repoRoot = "C:\Users\iva.kucerova\Eurofondy"
$dest = Join-Path $repoRoot "data\eufunds.duckdb"
$logFile = Join-Path $repoRoot "data\sync_log.txt"

function Write-Log($msg) {
    "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg" | Add-Content -Path $logFile
}

try {
    $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$repo/releases/tags/data-store" `
        -Headers @{ "User-Agent" = "eurofondy-local-sync" }

    $asset = $release.assets | Where-Object { $_.name -eq "eufunds.duckdb" }
    if (-not $asset) {
        Write-Log "No eufunds.duckdb asset found in the data-store release yet. Skipping."
        exit 0
    }

    $tmp = "$dest.tmp"
    Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $tmp -UseBasicParsing
    Move-Item -Path $tmp -Destination $dest -Force

    $sizeMb = [math]::Round($asset.size / 1MB, 2)
    Write-Log "Synced eufunds.duckdb successfully ($sizeMb MB, asset updated $($asset.updated_at))."
}
catch {
    Write-Log "Sync failed: $_"
    exit 1
}
