/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <reader/tsfile_reader.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "bench_mark_c_cpp.h"
#include "bench_mark_utils.h"
#include "common/db_common.h"
#include "common/path.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "utils/db_utils.h"
#include "writer/tsfile_table_writer.h"

using namespace storage;
using namespace common;

std::vector<std::string> columns_name;
std::vector<TSDataType> data_types;
using json = nlohmann::json;

json result;
void save_result_to_json() {
    std::ofstream file("/result/results_cpp.json");
    file << std::setw(2) << result << std::endl;
    file.close();
}

TableSchema* gen_table_schema(const std::vector<int>& field_type_vector) {
    std::vector<common::ColumnSchema> schemas;
    // 2 TAG Columns default
    for (int i = 0; i < 2; i++) {
        std::string column_name = std::string("TAG" + std::to_string(i));
        schemas.emplace_back(column_name, common::TSDataType::STRING,
                             common::ColumnCategory::TAG);
        columns_name.push_back(column_name);
        data_types.push_back(TSDataType::STRING);
    }

    int i = 2;
    int index = 0;
    for (int column_num : field_type_vector) {
        for (int j = 0; j < column_num; j++) {
            auto column_name = std::string("FIELD" + std::to_string(i++));
            auto type = static_cast<TSDataType>(data_type[index]);
            data_types.push_back(type);
            columns_name.push_back(column_name);
            schemas.emplace_back(column_name, type, ColumnCategory::FIELD);
        }
        index++;
    }
    return new TableSchema("TestTable", schemas);
}

int bench_mark_cpp_write() {
    int code = common::E_OK;
    Config config = load_config("/tmp/config.json");
    print_config(true, config);
    remove("tsfile_table_write_bench_mark_cpp.tsfile");
    libtsfile_init();
    // benchmark for write
    WriteFile file = WriteFile();

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    code = file.create("tsfile_table_write_bench_mark_cpp.tsfile", flags, mode);
    if (code != common::E_OK) {
        return -1;
    }

    std::ofstream csv_file("memory_usage_cpp.csv");
    if (!csv_file.is_open()) {
        std::cout << "csv create failed!" << std::endl;
        return 0;
    }
    csv_file << "iter_num,memory_usage(kb)\n";
    int iter_num = 0;
    csv_file << iter_num++ << "," << get_memory_usage() << "\n";

    TableSchema* table_schema = gen_table_schema(config.field_type_vector);
    auto writer = new TsFileTableWriter(&file, table_schema);
    delete (table_schema);

    int64_t timestamp = 0;
    int64_t prepare_time = 0;
    int64_t writing_time = 0;

    for (int tablet_i = 0; tablet_i < config.tablet_num; tablet_i++) {
        csv_file << iter_num++ << "," << get_memory_usage() << "\n";
        // print_progress_bar(tablet_i, config.tablet_num);
        auto prepare_start = std::chrono::high_resolution_clock::now();
        auto tablet = Tablet(
            columns_name, data_types,
            config.tag1_num * config.tag2_num * config.timestamp_per_tag);
        int row_num = 0;
        for (int tag1 = 0; tag1 < config.tag1_num; tag1++) {
            for (int tag2 = 0; tag2 < config.tag2_num; tag2++) {
                for (int row = 0; row < config.timestamp_per_tag; row++) {
                    tablet.add_timestamp(row_num, timestamp + row);
                    tablet.add_value(
                        row_num, 0,
                        std::string("tag1_" + std::to_string(tag1)).c_str());
                    tablet.add_value(
                        row_num, 1,
                        std::string("tag2_" + std::to_string(tag2)).c_str());
                    for (int col = 2; col < data_types.size(); col++) {
                        switch (data_types[col]) {
                            case TSDataType::INT32:
                                tablet.add_value(
                                    row_num, col,
                                    static_cast<int32_t>(timestamp));
                                break;
                            case TSDataType::INT64:
                                tablet.add_value(
                                    row_num, col,
                                    static_cast<int64_t>(timestamp));
                                break;
                            case TSDataType::FLOAT:
                                tablet.add_value(
                                    row_num, col,
                                    static_cast<float>(timestamp * 1.1));
                                break;
                            case TSDataType::DOUBLE:
                                tablet.add_value(
                                    row_num, col,
                                    static_cast<double>(timestamp * 1.1));
                                break;

                            case TSDataType::BOOLEAN:
                                tablet.add_value(
                                    row_num, col,
                                    static_cast<bool>(timestamp % 2));
                                break;
                            default:;
                        }
                    }
                    row_num++;
                }
            }
        }
        csv_file << iter_num++ << "," << get_memory_usage() << "\n";
        auto prepare_end = std::chrono::high_resolution_clock::now();

        prepare_time += std::chrono::duration_cast<std::chrono::microseconds>(
                            prepare_end - prepare_start)
                            .count();

        auto write_start = std::chrono::high_resolution_clock::now();
        writer->write_table(tablet);
        auto write_end = std::chrono::high_resolution_clock::now();
        writing_time += std::chrono::duration_cast<std::chrono::microseconds>(
                            write_end - write_start)
                            .count();
        timestamp += config.timestamp_per_tag;
        csv_file << iter_num++ << "," << get_memory_usage() << "\n";
    }

    csv_file << iter_num++ << "," << get_memory_usage() << "\n";
    auto close_start = std::chrono::high_resolution_clock::now();
    writer->flush();
    csv_file << iter_num++ << "," << get_memory_usage() << "\n";
    writer->close();
    csv_file << iter_num++ << "," << get_memory_usage() << "\n";
    auto close_end = std::chrono::high_resolution_clock::now();

    writing_time += std::chrono::duration_cast<std::chrono::microseconds>(
                        close_end - close_start)
                        .count();
    delete writer;

    FILE* file_to_size =
        fopen("tsfile_table_write_bench_mark_cpp.tsfile", "rb");
    if (!file_to_size) {
        std::cout << "unable to open file" << std::endl;
        return -1;
    }
    csv_file << iter_num++ << "," << get_memory_usage() << "\n";
    fseeko(file_to_size, 0, SEEK_END);
    off_t size = ftello(file_to_size);
    fclose(file_to_size);

    std::cout << "Finish writing for CPP" << std::endl;
    std::cout << "Tsfile size is " << size << " bytes " << " ~ " << size / 1024
              << "KB" << std::endl;

    double pre_time = prepare_time / 1000.0 / 1000.0;
    double write_time = writing_time / 1000.0 / 1000.0;
    double writing_speed = static_cast<long long>(
        config.tablet_num * config.tag1_num * config.tag2_num *
        config.timestamp_per_tag * (data_types.size() - 2) /
        (pre_time + write_time));
    std::cout << "Preparing time is " << pre_time << " s" << std::endl;
    std::cout << "Writing time is " << write_time << " s" << std::endl;
    std::cout << "writing speed is " << writing_speed << " points/s"
              << std::endl;

    result["tsfile_size"] = size / 1024;
    result["prepare_time"] = pre_time;
    result["write_time"] = write_time;
    result["writing_speed"] = writing_time;
    return 0;
}

int bench_mark_cpp_read() {
    libtsfile_init();
    int code = common::E_OK;
    TsFileReader reader = TsFileReader();
    reader.open("tsfile_table_write_bench_mark_cpp.tsfile");
    ResultSet* result_set = nullptr;
    code = reader.query("TestTable", columns_name, INT64_MIN, INT64_MAX,
                        result_set);
    bool has_next = false;
    int row = 0;
    auto read_start = std::chrono::high_resolution_clock::now();
    std::unordered_map<std::string, std::string> columns_info;
    while ((code = result_set->next(has_next)) == common::E_OK && has_next) {
        row++;
    }
    result_set->close();
    reader.close();
    delete result_set;
    auto read_end = std::chrono::high_resolution_clock::now();
    int64_t total_points = row * (columns_name.size() - 2);
    double reading_time = std::chrono::duration_cast<std::chrono::microseconds>(
                              read_end - read_start)
                              .count() /
                          1000.0 / 1000.0;

    std::cout << "total points is " << total_points << std::endl;
    std::cout << "reading time is " << reading_time << " s" << std::endl;
    std::cout << "read speed:"
              << static_cast<int64_t>(total_points / reading_time)
              << " points/s" << std::endl;
    result["reading_time"] = reading_time;
    result["reading_speed"] = static_cast<int64_t>(total_points / reading_time);

    return 0;
}
