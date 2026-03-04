#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/.."
ROOT_DIR="$(cd "$ROOT_DIR" && pwd)"

RESULT_DIR="${RESULT_DIR:-$ROOT_DIR/result}"
TSFILE_DIR="${TSFILE_DIR:-$ROOT_DIR/tsfile}"
BENCH_CORE_DIR="$ROOT_DIR/benchmark_core"

RUN_BENCH_SH="$ROOT_DIR/docker/run_benchmark.sh"

echo "ROOT_DIR      = $ROOT_DIR"
echo "TSFILE_DIR    = $TSFILE_DIR"
echo "BENCH_CORE    = $BENCH_CORE_DIR"
echo "RESULT_DIR    = $RESULT_DIR"
echo "RUN_BENCH_SH  = $RUN_BENCH_SH"

[ -d "$BENCH_CORE_DIR" ] || { echo "Missing BENCH_CORE_DIR: $BENCH_CORE_DIR"; exit 1; }
[ -f "$RUN_BENCH_SH" ]   || { echo "Missing RUN_BENCH_SH: $RUN_BENCH_SH"; exit 1; }

mkdir -p "$RESULT_DIR"

if [ -d "$TSFILE_DIR" ]; then
  rm -rf "$TSFILE_DIR"
  echo "TsFile directory found. Removed it."
fi

git clone git@github.com:ColinLeeo/tsfile.git "$TSFILE_DIR"

docker run  --rm --privileged \
  -v "$TSFILE_DIR:/workspace/tsfile" \
  -v "$BENCH_CORE_DIR:/workspace/benchmark_core" \
  -v "$RESULT_DIR:/result" \
  -v "$RUN_BENCH_SH:/workspace/run_benchmark.sh:ro" \
  -w /workspace \
  tsfile_benchmark:latest \
  bash /workspace/run_benchmark.sh
