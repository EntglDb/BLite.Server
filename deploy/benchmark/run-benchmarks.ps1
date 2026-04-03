# BLite.Server.Benchmarks — build + run helper (Windows PowerShell)
# Copyright (C) 2026 Luca Fabbri — AGPL-3.0
#
# Prerequisites:
#   - Docker Desktop running
#   - Both BLite/ and BLite.Server/ cloned into the same parent directory
#   - .NET 10 SDK installed
#
# Usage:
#   .\run-benchmarks.ps1                              — run CrudBenchmarks + QueryBenchmarks (default)
#   .\run-benchmarks.ps1 -BdnArgs "--filter","*Crud*" — run only CrudBenchmarks
#   .\run-benchmarks.ps1 -SkipBuild                   — reuse cached Docker image
#   .\run-benchmarks.ps1 -SkipDocker                  — skip Docker; use already-running services

param(
    [string[]] $BdnArgs    = @("--filter", "*Benchmarks*"),
    [switch]   $SkipBuild,
    [switch]   $SkipDocker
)

$ErrorActionPreference = 'Stop'

$ScriptDir     = $PSScriptRoot
$RepoDir       = (Resolve-Path "$ScriptDir\..\..").Path
$ParentDir     = (Resolve-Path "$RepoDir\..").Path
$ComposeFile   = "$ScriptDir\docker-compose.benchmark.yml"
$BenchmarkProj = "$RepoDir\tests\BLite.Server.Benchmarks\BLite.Server.Benchmarks.csproj"

if (-not $SkipDocker) {
    if (-not $SkipBuild) {
        Write-Host "▶  Building blite-server-bench image (context: $ParentDir)..."
        $ErrorActionPreference = 'Continue'
        docker build -f "$RepoDir\Dockerfile" -t blite-server-bench:latest $ParentDir
        $ErrorActionPreference = 'Stop'
        if ($LASTEXITCODE -ne 0) { throw "docker build failed (exit $LASTEXITCODE)" }
    }

    Write-Host "▶  Starting benchmark containers..."
    $ErrorActionPreference = 'Continue'
    docker compose -f $ComposeFile up -d
    $ErrorActionPreference = 'Stop'
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed (exit $LASTEXITCODE)" }

    Write-Host "⏳  Waiting for services to be ready (gRPC :2626, Mongo :27017)..."
    $deadline = (Get-Date).AddSeconds(90)
    $bliteOk = $false ; $mongoOk = $false
    while ((Get-Date) -lt $deadline) {
        if (-not $bliteOk) {
            $bliteOk = (Test-NetConnection -ComputerName localhost -Port 2626 -WarningAction SilentlyContinue).TcpTestSucceeded
        }
        if (-not $mongoOk) {
            $mongoOk = (Test-NetConnection -ComputerName localhost -Port 27017 -WarningAction SilentlyContinue).TcpTestSucceeded
        }
        if ($bliteOk -and $mongoOk) { break }
        Start-Sleep -Seconds 3
    }
    if (-not $bliteOk -or -not $mongoOk) {
        Write-Warning "Services did not open ports in time; running benchmarks anyway."
    } else {
        # Extra pause: port open != server ready (gRPC handshake needs a moment).
        Start-Sleep -Seconds 5
    }
}

# BenchmarkDotNet searches for the .csproj starting from Environment.CurrentDirectory.
# Push to the project directory so it finds BLite.Server.Benchmarks.csproj.
$BenchProjDir = Split-Path $BenchmarkProj -Parent
Push-Location $BenchProjDir
try {
    Write-Host "🚀  Running benchmarks..."
    # Pass --project so the correct project is used even though CWD changed.
    dotnet run --project $BenchmarkProj -c Release -- @BdnArgs
}
finally {
    Pop-Location
    if (-not $SkipDocker) {
        Write-Host "⏹  Stopping containers..."
        $ErrorActionPreference = 'Continue'
        docker compose -f $ComposeFile down
        $ErrorActionPreference = 'Stop'
    }
}

Write-Host "✅  Done. Results are in $BenchProjDir\BenchmarkDotNet.Artifacts/"
