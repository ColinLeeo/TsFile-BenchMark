#!/bin/bash
set -e 

echo "Starting building and running the benchmark..."

cd /workspace/tsfile

# build TsFile java and install it.
./mvnw install -P with-java -DskipTests

# build TsFile cpp and python
./mvnw clean package -P with-cpp,with-python -DskipTests

# install TsFile cpp
cp /workspace/tsfile/cpp/target/build/lib/libtsfile.so.2.2.0.dev /usr/local/lib/libtsfile.so.2.2.0.dev
ln -s /usr/local/lib/libtsfile.so.2.2.0.dev /usr/local/lib/libtsfile.so
cp -rf /workspace/tsfile/cpp/target/build/include /usr/local/include/tsfile 
echo "/usr/local/lib" > /etc/ld.so.conf.d/tsfile.conf
ldconfig

# install TsFile python
pip install /workspace/tsfile/python/dist/tsfile-2.2.0.dev0-cp310-cp310-linux_x86_64.whl

# run the benchmark

cd /workspace/benchmark_core/java
./mvnw clean package -DskipTests && java -jar target/benchmark-1.0-SNAPSHOT.jar
cp ./memory_usage_java.csv /result/

cd /workspace/benchmark_core/cpp
bash build.sh && /workspace/benchmark_core/cpp/build/Release/bench_mark
cp ./memory_usage_cpp.csv /result/

cd /workspace/benchmark_core/python
python3 benchmark.py
cp ./memory_usage_python.csv /result/

cd /workspace/tsfile
./mvnw clean package -P with-cpp -DskipTests -Dbuild.type=Debug
cp /workspace/tsfile/cpp/target/build/lib/libtsfile.so.2.2.0.dev /usr/local/lib/libtsfile.so.2.2.0.dev
cd /workspace/benchmark_core/cpp/build/Release

perf record -F 99 -g -- ./benchmark
perf script > /result/perf_cpp.perf

cd /workspace/benchmark_core/python
perf record -F 99 -g -- python3 benchmark.py
perf script > /result/perf_python.perf


/workspace/FlameGraph/stackcollapse-perf.pl /result/perf_cpp.perf > /result/cpp_flamegraph.txt
/workspace/FlameGraph/flamegraph.pl /result/cpp_flamegraph.txt > /result/cpp_flamegraph.svg
/workspace/FlameGraph/stackcollapse-perf.pl /result/perf_python.perf > /result/python_flamegraph.txt
/workspace/FlameGraph/flamegraph.pl /result/python_flamegraph.txt > /result/python_flamegraph.svg

echo "Benchmark completed successfully."








