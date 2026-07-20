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
while read -r target exact; do
    if [[ -z "${target:-}" || "$target" == \#* ]]; then
        continue
    fi
    run_integration "$target" "$exact"
done < "$target_file"
