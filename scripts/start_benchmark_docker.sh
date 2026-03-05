#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/.."
ROOT_DIR="$(cd "$ROOT_DIR" && pwd)"

RESULT_DIR="${RESULT_DIR:-$ROOT_DIR/result}"
TSFILE_DIR="${TSFILE_DIR:-$ROOT_DIR/tsfile}"
BENCH_CORE_DIR="$ROOT_DIR/benchmark_core"

RUN_BENCH_SH="$ROOT_DIR/docker/run_benchmark.sh"
DOCKERFILE_PATH="$ROOT_DIR/docker/Dockerfile"
IMAGE_NAME="${IMAGE_NAME:-tsfile_benchmark:latest}"

HTTP_PROXY_VALUE="${HTTP_PROXY:-${http_proxy:-}}"
HTTPS_PROXY_VALUE="${HTTPS_PROXY:-${https_proxy:-}}"
NO_PROXY_VALUE="${NO_PROXY:-${no_proxy:-localhost,127.0.0.1}}"

rewrite_local_proxy() {
  local proxy_value="$1"
  if [[ -z "$proxy_value" ]]; then
    echo ""
    return
  fi
  echo "$proxy_value" | sed -E 's#(https?://)(localhost|127\.0\.0\.1)(:[0-9]+)?#\1host.docker.internal\3#g'
}

HTTP_PROXY_FOR_DOCKER="$(rewrite_local_proxy "$HTTP_PROXY_VALUE")"
HTTPS_PROXY_FOR_DOCKER="$(rewrite_local_proxy "$HTTPS_PROXY_VALUE")"

BUILD_PROXY_ARGS=()
RUN_PROXY_ARGS=()
DOCKER_EXTRA_HOST_ARGS=()

if [[ -n "$HTTP_PROXY_FOR_DOCKER" || -n "$HTTPS_PROXY_FOR_DOCKER" ]]; then
  DOCKER_EXTRA_HOST_ARGS+=(--add-host host.docker.internal:host-gateway)
fi

if [[ -n "$HTTP_PROXY_FOR_DOCKER" ]]; then
  BUILD_PROXY_ARGS+=(--build-arg "HTTP_PROXY=$HTTP_PROXY_FOR_DOCKER" --build-arg "http_proxy=$HTTP_PROXY_FOR_DOCKER")
  RUN_PROXY_ARGS+=(-e "HTTP_PROXY=$HTTP_PROXY_FOR_DOCKER" -e "http_proxy=$HTTP_PROXY_FOR_DOCKER")
fi

if [[ -n "$HTTPS_PROXY_FOR_DOCKER" ]]; then
  BUILD_PROXY_ARGS+=(--build-arg "HTTPS_PROXY=$HTTPS_PROXY_FOR_DOCKER" --build-arg "https_proxy=$HTTPS_PROXY_FOR_DOCKER")
  RUN_PROXY_ARGS+=(-e "HTTPS_PROXY=$HTTPS_PROXY_FOR_DOCKER" -e "https_proxy=$HTTPS_PROXY_FOR_DOCKER")
fi

if [[ -n "$NO_PROXY_VALUE" ]]; then
  BUILD_PROXY_ARGS+=(--build-arg "NO_PROXY=$NO_PROXY_VALUE" --build-arg "no_proxy=$NO_PROXY_VALUE")
  RUN_PROXY_ARGS+=(-e "NO_PROXY=$NO_PROXY_VALUE" -e "no_proxy=$NO_PROXY_VALUE")
fi

echo "ROOT_DIR      = $ROOT_DIR"
echo "TSFILE_DIR    = $TSFILE_DIR"
echo "BENCH_CORE    = $BENCH_CORE_DIR"
echo "RESULT_DIR    = $RESULT_DIR"
echo "RUN_BENCH_SH  = $RUN_BENCH_SH"
echo "DOCKERFILE    = $DOCKERFILE_PATH"
echo "IMAGE_NAME    = $IMAGE_NAME"
echo "HTTP_PROXY    = ${HTTP_PROXY_FOR_DOCKER:-<empty>}"
echo "HTTPS_PROXY   = ${HTTPS_PROXY_FOR_DOCKER:-<empty>}"
echo "NO_PROXY      = ${NO_PROXY_VALUE:-<empty>}"

[ -d "$BENCH_CORE_DIR" ] || { echo "Missing BENCH_CORE_DIR: $BENCH_CORE_DIR"; exit 1; }
[ -f "$RUN_BENCH_SH" ]   || { echo "Missing RUN_BENCH_SH: $RUN_BENCH_SH"; exit 1; }
[ -f "$DOCKERFILE_PATH" ] || { echo "Missing Dockerfile: $DOCKERFILE_PATH"; exit 1; }

mkdir -p "$RESULT_DIR"

if [ -d "$TSFILE_DIR" ]; then
  rm -rf "$TSFILE_DIR"
  echo "TsFile directory found. Removed it."
fi

git clone git@github.com:ColinLeeo/tsfile.git "$TSFILE_DIR"

echo "Building Docker image: $IMAGE_NAME"
# docker build "${DOCKER_EXTRA_HOST_ARGS[@]}" "${BUILD_PROXY_ARGS[@]}" -t "$IMAGE_NAME" -f "$DOCKERFILE_PATH" "$ROOT_DIR"

docker run  --rm --privileged \
  "${DOCKER_EXTRA_HOST_ARGS[@]}" \
  "${RUN_PROXY_ARGS[@]}" \
  -v "$TSFILE_DIR:/workspace/tsfile" \
  -v "$BENCH_CORE_DIR:/workspace/benchmark_core" \
  -v "$RESULT_DIR:/result" \
  -v "$RUN_BENCH_SH:/workspace/run_benchmark.sh:ro" \
  -w /workspace \
  "$IMAGE_NAME" \
  bash /workspace/run_benchmark.sh
