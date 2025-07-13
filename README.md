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