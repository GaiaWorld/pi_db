param(
    [ValidateSet("run", "list")]
    [string]$Mode = "run"
)

$ErrorActionPreference = "Stop"
$RepoDir = Split-Path -Parent $PSScriptRoot
$TargetFile = Join-Path $PSScriptRoot "new-regression-targets.txt"
$CargoBin = if ($env:CARGO_BIN) { $env:CARGO_BIN } else { "cargo" }
$Toolchain = if ($env:PI_DB_TOOLCHAIN) { $env:PI_DB_TOOLCHAIN } else { "nightly-2026-06-25" }

function Invoke-CargoChecked {
    param([string[]]$Arguments)

    & $CargoBin @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "cargo exited with code $LASTEXITCODE"
    }
}

Push-Location $RepoDir
try {
    $LibArguments = @("+$Toolchain", "test", "--locked", "--offline", "-p", "pi_db", "--lib", "--")
    if ($Mode -eq "list") {
        $LibArguments += @("--list", "--format", "terse")
    } else {
        $LibArguments += "--test-threads=1"
    }
    Invoke-CargoChecked $LibArguments

    foreach ($Line in Get-Content $TargetFile) {
        $Trimmed = $Line.Trim()
        if (-not $Trimmed -or $Trimmed.StartsWith("#")) {
            continue
        }
        $Parts = $Trimmed -split "\s+", 2
        $Target = $Parts[0]
        $Exact = $Parts[1]
        $Arguments = @(
            "+$Toolchain", "test", "--locked", "--offline", "-p", "pi_db",
            "--test", $Target, "--"
        )
        if ($Exact -ne "-") {
            $Arguments += @("--exact", $Exact)
        }
        if ($Mode -eq "list") {
            $Arguments += @("--list", "--format", "terse")
        } else {
            $Arguments += "--test-threads=1"
        }
        Invoke-CargoChecked $Arguments
    }
} finally {
    Pop-Location
}
