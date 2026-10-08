<#
.SYNOPSIS
    PowerShell equivalent of the Makefile for users without `make` on Windows.

.EXAMPLE
    .\make.ps1 build
    .\make.ps1 up
    .\make.ps1 logs
    .\make.ps1 down
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'build', 'up', 'down', 'restart', 'logs', 'ps', 'bootstrap', 'shell', 'clean', 'nuke')]
    [string]$Target = 'help'
)

$ErrorActionPreference = 'Stop'

function Get-GitInfo {
    try   { $branch = (git rev-parse --abbrev-ref HEAD 2>$null).Trim() } catch { $branch = 'unknown' }
    try   { $sha    = (git rev-parse --short HEAD       2>$null).Trim() } catch { $sha    = 'unknown' }
    if (-not $branch) { $branch = 'unknown' }
    if (-not $sha)    { $sha    = 'unknown' }
    return @{ Branch = $branch; Sha = $sha }
}

$git = Get-GitInfo
$env:GIT_BRANCH = $git.Branch
$env:GIT_SHA    = $git.Sha

$engine = 'podman'
$composePrefix = @('compose', '--env-file', 'docker/.env.docker')

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments = $true)]$ComposeArgs)
    # Flatten in case the caller passed an array literal as one positional arg
    $flat = @()
    foreach ($a in $ComposeArgs) { $flat += $a }
    $all = $composePrefix + $flat
    & $engine @all
    if ($LASTEXITCODE -ne 0) { throw "$engine compose failed with exit code $LASTEXITCODE" }
}

switch ($Target) {
    'help' {
        Write-Host "Pangolin container helper (PowerShell, engine: $engine). Targets:" -ForegroundColor Cyan
        Write-Host "  build      Build image, baking GIT_BRANCH=$($git.Branch) GIT_SHA=$($git.Sha)"
        Write-Host "  up         Start the full stack"
        Write-Host "  down       Stop the stack (keep volumes)"
        Write-Host "  restart    Restart worker only"
        Write-Host "  logs       Tail logs (Ctrl+C to exit)"
        Write-Host "  ps         Show service status"
        Write-Host "  bootstrap  Re-run bootstrap one-shot (idempotent)"
        Write-Host "  shell      Open a shell in the worker container"
        Write-Host "  clean      Stop stack and drop named volumes (WIPES Prefect DB)"
        Write-Host "  nuke       clean + remove the pangolin image"
        Write-Host ""
        Write-Host "Build a specific branch:"
        Write-Host "    git checkout <branch>; .\make.ps1 build"
    }
    'build'     { $env:BUILDX_NO_DEFAULT_ATTESTATIONS = '1'; Invoke-Compose @('build', '--build-arg', "GIT_BRANCH=$($git.Branch)", '--build-arg', "GIT_SHA=$($git.Sha)") }
    'up'        { Invoke-Compose @('up', '-d') }
    'down'      { Invoke-Compose @('down') }
    'restart'   { Invoke-Compose @('restart', 'worker') }
    'logs'      { Invoke-Compose @('logs', '-f', '--tail=100') }
    'ps'        { Invoke-Compose @('ps') }
    'bootstrap' { Invoke-Compose @('run', '--rm', 'bootstrap') }
    'shell'     { Invoke-Compose @('exec', 'worker', '/bin/bash') }
    'clean'     { Invoke-Compose @('down', '-v') }
    'nuke' {
        Invoke-Compose @('down', '-v')
        & $engine image rm "pangolin:$($git.Branch)" 2>$null
    }
}
