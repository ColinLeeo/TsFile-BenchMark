#!/bin/bash
# Build Parquet C++ benchmark (requires Arrow and Parquet C++ to be installed)
set -e
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)"
