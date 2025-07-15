## BenchMark For TsFile multi-language


### Output
The benchmark generates the following output files:

`benchmark_language.json` – A summary file in JSON format containing:
    Data preparation time(s).
    Data writing time(s).
    Total execution time(s).
    Writing throughput (points/s)

`memory_usage_{language}.csv` – A CSV file tracking memory usage over time during execution.

Profile output files (for Python and C++):
Flamegraph-related profiling data will be generated to assist with performance analysis.


### requiement

python: tqdm



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
   