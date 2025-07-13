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
#
import os
from time import perf_counter
from dataclasses import dataclass
import json

from tqdm import tqdm
import psutil
import csv
from tsfile import TSDataType, ColumnCategory
from tsfile import TableSchema, ColumnSchema
from tsfile import Tablet
from tsfile import TsFileTableWriter, TsFileReader

@dataclass
class Config:
    tablet_num: int
    tag1_num: int
    tag2_num: int
    timestamp_per_tag: int
    field_type_vector: list[int]

def load_config(config_path: str) -> Config:
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    return Config(**config_data)

type_list = [TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.BOOLEAN]


def print_config(config: Config):
    data_types_name = ["INT64", "INT32", "FLOAT", "DOUBLE", "BOOLEAN"]
    print("=====================")
    print("TsFile benchmark For Python")
    print("Schema Configuration:")
    print(f"Tag Column num: {2}")
    print(f"TAG1 num: {config.tag1_num} TAG2 num: {config.tag2_num}\n")

    print("Filed Column and types: ")
    column_num = 0
    for i in range(5):
        print(f"{data_types_name[i]}x{config.field_type_vector[i]}  ", end="")
        column_num += config.field_type_vector[i]

    print("\n")
    print(f"Tablet num: {config.tablet_num}")
    print(f"Tablet row num per tag: {config.timestamp_per_tag}")

    total_points = (config.tablet_num *
                    config.tag1_num *
                    config.tag2_num *
                    config.timestamp_per_tag *
                    column_num)
    print(f"Total points is {total_points}")
    print("======")

column_name = []

def get_memory_usage_kb():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss // 1024

def bench_mark_write():
    csv_file = "memory_usage_python.csv"

    csv_writer = csv.writer(open(csv_file, "w"))
    csv_writer.writerow(["iter_num", "memory_usage(kb)"])
    iter_num = 0
    print_config()
    column_schema_list = []
    column_datat_type = []
    column_schema_list.append(ColumnSchema("TAG1", TSDataType.STRING, ColumnCategory.TAG))
    column_name.append("TAG1")
    column_datat_type.append(TSDataType.STRING)
    column_schema_list.append(ColumnSchema("TAG2", TSDataType.STRING, ColumnCategory.TAG))
    column_name.append("TAG2")
    column_datat_type.append(TSDataType.STRING)


    i = 2
    for count, type in zip(config.field_type_vector, type_list):
        for _ in range(count):
            column_schema_list.append(ColumnSchema("FIELD" + str(i), type, ColumnCategory.FIELD))
            column_name.append("FIELD" + str(i))
            column_datat_type.append(type)
            i = i + 1

    timestamp = 0
    table_schema = TableSchema("TestTable", column_schema_list)
    start = perf_counter()
    prepare_time = 0
    writing_time = 0
    csv_writer.writerow([iter_num, get_memory_usage_kb()])
    iter_num += 1
    with TsFileTableWriter("tsfile_table_write_bench_mark.tsfile", table_schema) as writer:
        csv_writer.writerow([iter_num, get_memory_usage_kb()])
        iter_num += 1
        for i in tqdm(range(config.tablet_num), desc="Tablets"):
            csv_writer.writerow([iter_num, get_memory_usage_kb()])
            iter_num += 1
            row_num = 0
            prepare_start = perf_counter()
            tablet = Tablet(column_name, column_datat_type,
                            config.timestamp_per_tag * config.tag1_num *
                            config.tag2_num)

            for j in range(config.tag1_num):
                for k in range(config.tag2_num):
                    for row in range(config.timestamp_per_tag):
                        tablet.add_timestamp(row_num, timestamp + row)
                        tablet.add_value_by_index(0, row_num, "tag1_" + str(j))
                        tablet.add_value_by_index(1, row_num, "tag2_" + str(k))
                        for col in range(2, len(column_name)):
                            if column_datat_type[col] == TSDataType.INT32:
                                tablet.add_value_by_index(col, row_num, timestamp)
                            elif column_datat_type[col] == TSDataType.INT64:
                                tablet.add_value_by_index(col, row_num, timestamp)
                            elif column_datat_type[col] == TSDataType.FLOAT:
                                tablet.add_value_by_index(col, row_num, timestamp * 1.1)
                            elif column_datat_type[col] == TSDataType.DOUBLE:
                                tablet.add_value_by_index(col, row_num, timestamp * 1.1)
                            elif column_datat_type[col] == TSDataType.BOOLEAN:
                                tablet.add_value_by_index(col, row_num, timestamp % 2 == 0)
                        row_num = row_num + 1

            prepare_time += perf_counter() - prepare_start
            write_start = perf_counter()
            csv_writer.writerow([iter_num, get_memory_usage_kb()])
            iter_num += 1
            writer.write_table(tablet)
            writing_time += perf_counter() - write_start
            timestamp = timestamp + config.timestamp_per_tag
            csv_writer.writerow([iter_num, get_memory_usage_kb()])
            iter_num += 1

    csv_writer.writerow([iter_num, get_memory_usage_kb()])
    iter_num += 1
    end = perf_counter()
    total_time = end - start
    size = os.path.getsize("tsfile_table_write_bench_mark.tsfile")

    total_points = config.tablet_num * config.tag1_num * config.tag2_num * \
                   config.timestamp_per_tag * len(column_name)

    print("finish bench mark for python")
    print(f"tsfile size is {size} bytes ~ {size // 1024}KB")

    print(f"prepare data time is {prepare_time:.6f} s")
    print(f"writing data time is {writing_time:.6f} s")
    print(f" total_time is {total_time} s")
    writing_speed = int(total_points / (prepare_time + writing_time))
    print(f"writing speed is {writing_speed} points/s")

    total_time_seconds = (end - start)
    print(f"total time is {total_time_seconds:.6f} s")

def bench_mark_read():
    start = perf_counter()
    row = 0
    with TsFileReader("tsfile_table_write_bench_mark.tsfile") as reader:
        result = reader.query_table("TestTable", column_name)
        first = True
        while result.next():
            row = row + 1

    end = perf_counter()
    total_time = end - start
    reading_speed = int(row * len(column_name) / total_time)
    print("total row is ", row)
    print(f"reading data time is {total_time} s")
    print(f"reading data speed is {reading_speed} points/s")


bench_mark_write()
bench_mark_read()
