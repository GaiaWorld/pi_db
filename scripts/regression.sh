#!/usr/bin/env bash

set -euo pipefail

mode="${1:-run}"
case "$mode" in
    run|list) ;;
    *)
        printf 'usage: %s [run|list]\n' "$0" >&2
        exit 2
        ;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "$script_dir/.." && pwd)"
target_file="$script_dir/new-regression-targets.txt"
cargo_bin="${CARGO_BIN:-cargo}"
toolchain="${PI_DB_TOOLCHAIN:-nightly-2026-06-25}"

run_lib() {
    if [[ "$mode" == "list" ]]; then
        "$cargo_bin" "+$toolchain" test --locked --offline -p pi_db --lib -- \
            --list --format terse
    else
        "$cargo_bin" "+$toolchain" test --locked --offline -p pi_db --lib -- \
            --test-threads=1
    fi
}

# trace-only 指标、原子配平和真实 TTL 指标测试必须进入永久新回归，但不扩大到全部 integration。
run_trace_lib() {
    if [[ "$mode" == "list" ]]; then
        "$cargo_bin" "+$toolchain" test --locked --offline -p pi_db --features trace --lib -- \
            --list --format terse
    else
        "$cargo_bin" "+$toolchain" test --locked --offline -p pi_db --features trace --lib -- \
            --test-threads=1
    fi
}

# 真实跨 runtime TTL/publication 交错用于保护 trace 容量原子与 scanner 的并发配平。
run_trace_ttl_interleaving() {
    if [[ "$mode" == "list" ]]; then
        "$cargo_bin" "+$toolchain" test --locked --offline -p pi_db --features trace \
            --test key_version_ttl_index -- --list --format terse
    else
        "$cargo_bin" "+$toolchain" test --locked --offline -p pi_db --features trace \
            --test key_version_ttl_index -- --test-threads=1
    fi
}

# global MeterProvider 必须在数据库启动前生效；独立 target 同时校验六项指标和 loop INFO。
run_trace_meter_initialization() {
    if [[ "$mode" == "list" ]]; then
        "$cargo_bin" "+$toolchain" test --locked --offline -p pi_db --features trace \
            --test trace_meter_initialization -- --list --format terse
    else
        "$cargo_bin" "+$toolchain" test --locked --offline -p pi_db --features trace \
            --test trace_meter_initialization -- --test-threads=1
    fi
}

run_integration() {
    local target="$1"
    local exact="$2"
    local args=("+$toolchain" test --locked --offline -p pi_db --test "$target" --)

    if [[ "$exact" != "-" ]]; then
        args+=(--exact "$exact")
    fi
    if [[ "$mode" == "list" ]]; then
        args+=(--list --format terse)
    else
        args+=(--test-threads=1)
    fi
    "$cargo_bin" "${args[@]}"
}

cd "$repo_dir"
run_lib
run_trace_lib
run_trace_ttl_interleaving
run_trace_meter_initialization
while read -r target exact; do
    if [[ -z "${target:-}" || "$target" == \#* ]]; then
        continue
    fi
    run_integration "$target" "$exact"
done < "$target_file"
