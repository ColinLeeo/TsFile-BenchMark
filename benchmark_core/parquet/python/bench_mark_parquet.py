# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import json
import csv
from time import perf_counter
from dataclasses import dataclass

import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


@dataclass
class Config:
    tablet_num: int
    tag1_num: int
    tag2_num: int
    timestamp_per_tag: int
    field_type_vector: list  # 5 elements: BOOLEAN, INT32, INT64, FLOAT, DOUBLE counts


def load_config(config_path: str) -> Config:
    with open(config_path, "r") as f:
        data = json.load(f)
    return Config(**data)


# PyArrow types for field_type_vector index 0..4
PA_TYPES = [pa.bool_(), pa.int32(), pa.int64(), pa.float32(), pa.float64()]


def build_schema_and_field_names(config: Config):
    """Returns (pa.Schema, list of column names, num_field_columns)."""
    names = ["timestamp", "TAG1", "TAG2"]
    types = [pa.int64(), pa.string(), pa.string()]
    idx = 2
    for type_idx, count in enumerate(config.field_type_vector):
        for _ in range(count):
            names.append("FIELD" + str(idx))
            types.append(PA_TYPES[type_idx])
            idx += 1
    schema = pa.schema(zip(names, types))
    num_field_columns = len(names) - 3
    return schema, names, num_field_columns


def get_memory_usage_kb():
    return psutil.Process(os.getpid()).memory_info().rss // 1024


def bench_mark_write(config: Config, schema, names, num_field_columns,
                     csv_writer, iter_num_ref, results: dict):
    out_path = "bench_mark_parquet_python.parquet"
    if os.path.exists(out_path):
        os.remove(out_path)

    prepare_time = 0.0
    writing_time = 0.0
    timestamp = 0

    csv_writer.writerow([iter_num_ref[0], get_memory_usage_kb()])
    iter_num_ref[0] += 1

    with pq.ParquetWriter(out_path, schema) as writer:
        csv_writer.writerow([iter_num_ref[0], get_memory_usage_kb()])
        iter_num_ref[0] += 1

        for _ in tqdm(range(config.tablet_num), desc="Parquet Tablets"):
            csv_writer.writerow([iter_num_ref[0], get_memory_usage_kb()])
            iter_num_ref[0] += 1

            prepare_start = perf_counter()
            # Build columns: same order as TsFile (tag1, tag2, row in timestamp_per_tag)
            n = config.tag1_num * config.tag2_num * config.timestamp_per_tag
            cols = {names[0]: [], names[1]: [], names[2]: []}
            for i in range(3, len(names)):
                cols[names[i]] = []

            for tag1 in range(config.tag1_num):
                for tag2 in range(config.tag2_num):
                    for row in range(config.timestamp_per_tag):
                        t = timestamp + row
                        cols[names[0]].append(t)
                        cols[names[1]].append("tag1_" + str(tag1))
                        cols[names[2]].append("tag2_" + str(tag2))
                        col_idx = 3
                        for type_idx, count in enumerate(config.field_type_vector):
                            for _ in range(count):
                                if type_idx == 0:
                                    cols[names[col_idx]].append(t % 2 == 0)
                                elif type_idx == 1:
                                    cols[names[col_idx]].append(int(t))
                                elif type_idx == 2:
                                    cols[names[col_idx]].append(t)
                                elif type_idx == 3:
                                    cols[names[col_idx]].append(float(t) * 1.1)
                                else:
                                    cols[names[col_idx]].append(float(t) * 1.1)
                                col_idx += 1

            arrays = [pa.array(cols[names[i]]) for i in range(len(names))]
            table = pa.table(arrays, names=names)
            prepare_time += perf_counter() - prepare_start

            csv_writer.writerow([iter_num_ref[0], get_memory_usage_kb()])
            iter_num_ref[0] += 1

            write_start = perf_counter()
            writer.write_table(table)
            writing_time += perf_counter() - write_start
            timestamp += config.timestamp_per_tag

            csv_writer.writerow([iter_num_ref[0], get_memory_usage_kb()])
            iter_num_ref[0] += 1

    csv_writer.writerow([iter_num_ref[0], get_memory_usage_kb()])
    iter_num_ref[0] += 1

    size = os.path.getsize(out_path)
    total_points = (
        config.tablet_num * config.tag1_num * config.tag2_num
        * config.timestamp_per_tag * num_field_columns
    )
    writing_speed = int(total_points / (prepare_time + writing_time))

    print("Finish benchmark for Parquet (Python)")
    print(f"Parquet size is {size} bytes ~ {size // 1024} KB")
    print(f"Prepare time is {prepare_time:.6f} s")
    print(f"Writing time is {writing_time:.6f} s")
    print(f"Writing speed is {writing_speed} points/s")

    results["tsfile_size"] = size // 1024
    results["prepare_time"] = round(prepare_time, 6)
    results["write_time"] = round(writing_time, 6)
    results["writing_speed"] = writing_speed
    return out_path, num_field_columns


def bench_mark_read(path: str, num_field_columns: int, results: dict):
    start = perf_counter()
    row_count = 0
    with pq.ParquetFile(path) as pf:
        for batch in pf.iter_batches():
            row_count += batch.num_rows
    total_time = perf_counter() - start
    reading_speed = int(row_count * num_field_columns / total_time) if total_time > 0 else 0
    print("Total rows:", row_count)
    print(f"Reading time is {total_time:.6f} s")
    print(f"Reading speed is {reading_speed} points/s")
    results["reading_time"] = round(total_time, 6)
    results["reading_speed"] = reading_speed


def main():
    config = load_config("/tmp/conf.json")
    schema, names, num_field_columns = build_schema_and_field_names(config)

    csv_path = "memory_usage_parquet_python.csv"
    with open(csv_path, "w", newline="") as cf:
        csv_writer = csv.writer(cf)
        csv_writer.writerow(["iter_num", "memory_usage(kb)"])
        iter_num_ref = [0]
        results = {}

        out_path, nf = bench_mark_write(
            config, schema, names, num_field_columns,
            csv_writer, iter_num_ref, results
        )
        bench_mark_read(out_path, nf, results)

    result_dir = "/result"
    if os.path.isdir(result_dir):
        with open(os.path.join(result_dir, "results_parquet_python.json"), "w") as f:
            json.dump(results, f, indent=2)
    else:
        with open("results_parquet_python.json", "w") as f:
            json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
