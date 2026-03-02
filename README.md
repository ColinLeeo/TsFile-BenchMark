## BenchMark For TsFile multi-language

This repo compares **TsFile** and **Parquet** using the same config and metrics (prepare/write/read time, throughput, file size, memory).

### Project structure

```
benchmark_core/
├── conf.json              # Shared config (tablet_num, tag1_num, tag2_num, timestamp_per_tag, field_type_vector)
├── tsfile/                # TsFile benchmarks
│   ├── java/
│   ├── python/
│   └── cpp/
└── parquet/               # Parquet benchmarks
    ├── java/
    ├── python/
    └── cpp/
```

### Output

Results under `/result` (or `result/` when run locally):

| Format  | Language | Summary JSON              | Memory CSV                    |
|---------|----------|---------------------------|-------------------------------|
| TsFile  | Java     | `results_java.json`       | `memory_usage_java.csv`       |
| TsFile  | Python   | `results_python.json`     | `memory_usage_python.csv`     |
| TsFile  | C++      | (console)                 | `memory_usage_cpp.csv`        |
| Parquet | Java     | `results_parquet_java.json`   | `memory_usage_parquet_java.csv`  |
| Parquet | Python   | `results_parquet_python.json` | `memory_usage_parquet_python.csv` |
| Parquet | C++      | `results_parquet_cpp.json`    | `memory_usage_parquet_cpp.csv`   |

Each JSON contains: `tsfile_size` (KB), `prepare_time`, `write_time`, `writing_speed`, `reading_time`, `reading_speed`.

Profile output files (for Python and C++):
Flamegraph-related profiling data will be generated to assist with performance analysis.


### Requirements

- Python: `tqdm`, `psutil`, `pyarrow` (for Parquet Python)
- Java: Maven (for TsFile and Parquet Java)
- C++: CMake, TsFile C++ (for TsFile C++), Arrow C++ / Parquet C++ (for Parquet C++)



### Benchmark Execution Workflow
1. Clone TsFile Source Code

    Clone the latest TsFile repository from GitHub to your local machine.

2. Start Docker with Volume Mounts

    Launch a Docker container and mount the following directories into the container:
    1. The cloned TsFile source code
    2. The TsFile-benchmark repository itself
    3. A result directory, which should be mounted to /result inside the container to store output files

3. Build and Run Benchmarks in Docker
    The Docker container will automatically:

    1. Compile and install the TsFile project
    2. Run performance benchmarks
    3. Collect profiling data using perf. Flamegraph results (for C++ and Python) will be generated and stored under the /result directory.



