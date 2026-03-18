#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-current}"

usage() {
    echo "用法: bash tools/cleanup_quick_repair_dirs.sh [current|all]"
    echo "  current: 只删除统一后的 ./tmp_quick_repair 目录"
    echo "  all:     同时删除 ./tmp_quick_repair 和历史遗留的 ./tmp_quick_repair_* 目录"
}

remove_dir() {
    local path="$1"

    if [[ -d "$path" ]]; then
        rm -rf "$path"
        echo "removed: $path"
    fi
}

case "$MODE" in
    current)
        remove_dir "$ROOT_DIR/tmp_quick_repair"
        ;;
    all)
        remove_dir "$ROOT_DIR/tmp_quick_repair"
        while IFS= read -r -d '' path; do
            remove_dir "$path"
        done < <(find "$ROOT_DIR" -maxdepth 1 -type d -name 'tmp_quick_repair_*' -print0)
        ;;
    *)
        usage
        exit 1
        ;;
esac
