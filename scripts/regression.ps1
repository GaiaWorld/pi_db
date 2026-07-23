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

    # trace-only 指标/原子测试和真实 TTL 交错属于永久新回归；其它 integration 保持默认 feature。
    $TraceLibArguments = @(
        "+$Toolchain", "test", "--locked", "--offline", "-p", "pi_db",
        "--features", "trace", "--lib", "--"
    )
    if ($Mode -eq "list") {
        $TraceLibArguments += @("--list", "--format", "terse")
    } else {
        $TraceLibArguments += "--test-threads=1"
    }
    Invoke-CargoChecked $TraceLibArguments

    $TraceTtlArguments = @(
        "+$Toolchain", "test", "--locked", "--offline", "-p", "pi_db",
        "--features", "trace", "--test", "key_version_ttl_index", "--"
    )
    if ($Mode -eq "list") {
        $TraceTtlArguments += @("--list", "--format", "terse")
    } else {
        $TraceTtlArguments += "--test-threads=1"
    }
    Invoke-CargoChecked $TraceTtlArguments

    # 独立验证 global MeterProvider 初始化顺序、共享 scope、六项指标和 loop INFO。
    $TraceMeterArguments = @(
        "+$Toolchain", "test", "--locked", "--offline", "-p", "pi_db",
        "--features", "trace", "--test", "trace_meter_initialization", "--"
    )
    if ($Mode -eq "list") {
        $TraceMeterArguments += @("--list", "--format", "terse")
    } else {
        $TraceMeterArguments += "--test-threads=1"
    }
    Invoke-CargoChecked $TraceMeterArguments

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
