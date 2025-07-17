#!/bin/bash

set -e

if [-d "tsfile"]; then
    rm -rf tsfile
    echo "TsFile directory found. Remove it"
fi

# Clone the TsFile repository
git clone https://github.com/apache/tsfile.git

docker run --rm --privileged \
    -v "$(pwd)/tsfile:/workspace/tsfile" \
    -v "/home/colin/dev/TsFile-BenchMark/benchmark_core:/workspace/benchmark_core" \
    -v "$(pwd)/result:/result" \
    -v "/home/colin/dev/TsFile-BenchMark/benchmark_core/config:/tmp/config" \
    -v "/home/colin/dev/FlameGraph:/workspace/FlameGraph" \
    -v "/home/colin/dev/TsFile-BenchMark/docker/run_benchmark.sh:/workspace/run_benchmark.sh" \
    -w /workspace \
    tsfile-benchmark:latest \
    bash -c "chmod +x run_benchmark.sh && ./run_benchmark.sh"
