#!/bin/bash
set -e

echo "Starting building and running the benchmark..."

# Ensure config is available for all benchmarks
cp /workspace/benchmark_core/conf.json /tmp/conf.json

cd /workspace/tsfile
mvn -N io.takari:maven:wrapper -Dmaven=3.9.6
./mvnw install -P with-java -DskipTests
./mvnw clean package -P with-cpp,with-python -DskipTests

# Install TsFile C++ and Python
cp /workspace/tsfile/cpp/target/build/lib/libtsfile.so.2.*.*.dev /usr/local/lib/libtsfile.so.2.*.*.dev
ln -sf /usr/local/lib/libtsfile.so.2.*.*.dev /usr/local/lib/libtsfile.so
cp -rf /workspace/tsfile/cpp/target/build/include /usr/local/include/tsfile
echo "/usr/local/lib" > /etc/ld.so.conf.d/tsfile.conf
ldconfig
pip install /workspace/tsfile/python/dist/tsfile-2.*.*.dev0-cp310-cp310-linux_x86_64.whl

# ---------- TsFile benchmarks ----------
echo "Running TsFile benchmarks..."

cd /workspace/benchmark_core/tsfile/java
mvn clean package -DskipTests && java -jar target/benchmark-1.0-SNAPSHOT.jar
cp -f memory_usage_java.csv /result/ 2>/dev/null || true

cd /workspace/benchmark_core/tsfile/cpp
bash build.sh && /workspace/benchmark_core/tsfile/cpp/build/Release/bench_mark
cp -f memory_usage_cpp.csv /result/ 2>/dev/null || true

cd /workspace/benchmark_core/tsfile/python
python3 bench_mark.py
cp -f memory_usage_python.csv /result/ 2>/dev/null || true

# ---------- Parquet benchmarks ----------
echo "Running Parquet benchmarks..."

cd /workspace/benchmark_core/parquet/java
mvn clean package -DskipTests && java -jar target/benchmark-parquet-1.0-SNAPSHOT.jar
cp -f memory_usage_parquet_java.csv /result/ 2>/dev/null || true

cd /workspace/benchmark_core/parquet/python
python3 bench_mark_parquet.py
cp -f memory_usage_parquet_python.csv /result/ 2>/dev/null || true

cd /workspace/benchmark_core/parquet/cpp
bash build.sh && ./build/bench_mark_parquet
cp -f memory_usage_parquet_cpp.csv /result/ 2>/dev/null || true

# ---------- Flamegraph (TsFile C++/Python only) ----------
if [ -d /workspace/FlameGraph ]; then
  cd /workspace/tsfile
  ./mvnw clean package -P with-cpp -DskipTests -Dbuild.type=Debug
  cp /workspace/tsfile/cpp/target/build/lib/libtsfile.so.2.*.*.dev /usr/local/lib/libtsfile.so.2.*.*.dev
  cd /workspace/benchmark_core/tsfile/cpp/build/Release
  perf record -F 99 -g -- ./bench_mark 2>/dev/null || true
  perf script > /result/perf_cpp.perf 2>/dev/null || true
  cd /workspace/benchmark_core/tsfile/python
  perf record -F 99 -g -- python3 bench_mark.py 2>/dev/null || true
  perf script > /result/perf_python.perf 2>/dev/null || true
  /workspace/FlameGraph/stackcollapse-perf.pl /result/perf_cpp.perf > /result/cpp_flamegraph.txt 2>/dev/null || true
  /workspace/FlameGraph/flamegraph.pl /result/cpp_flamegraph.txt > /result/cpp_flamegraph.svg 2>/dev/null || true
  /workspace/FlameGraph/stackcollapse-perf.pl /result/perf_python.perf > /result/python_flamegraph.txt 2>/dev/null || true
  /workspace/FlameGraph/flamegraph.pl /result/python_flamegraph.txt > /result/python_flamegraph.svg 2>/dev/null || true
fi

echo "Benchmark completed successfully."
