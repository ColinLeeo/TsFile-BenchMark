#!/bin/bash

set -e

if [ -d "tsfile" ]; then
    rm -rf tsfile
    echo "TsFile directory found. Remove it"
fi

# Clone the TsFile repository
git clone git@github.com:ColinLeeo/tsfile.git

docker run --rm --privileged \
    -v "$(pwd)/tsfile:/workspace/tsfile" \
    -v "/home/colin/dev/TsFile-BenchMark/benchmark_core:/workspace/benchmark_core" \
    -v "$(pwd)/result:/result" \
    -v "$(pwd)/../docker/run_benchmark.sh:/workspace/run_benchmark.sh" \
    -v "$(pwd)/../benchmark_core/conf.json:/tmp/conf.json" \
    -w /workspace \
    tsfile-benchmark:latest \
    bash -c "chmod +x run_benchmark.sh && ./run_benchmark.sh"
