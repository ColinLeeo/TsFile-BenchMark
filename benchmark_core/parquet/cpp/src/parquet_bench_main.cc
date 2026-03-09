/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "parquet_bench_utils.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <arrow/util/macros.h>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

arrow::Result<std::shared_ptr<arrow::Schema>> BuildSchema(const Config& config) {
  std::vector<std::shared_ptr<arrow::Field>> fields = {
      arrow::field("timestamp", arrow::int64()),
      arrow::field("TAG1", arrow::utf8()),
      arrow::field("TAG2", arrow::utf8()),
  };
  int field_idx = 2;
  for (size_t type_idx = 0; type_idx < config.field_type_vector.size(); type_idx++) {
    int count = config.field_type_vector[type_idx];
    std::shared_ptr<arrow::DataType> dt;
    switch (type_idx) {
      case 0: dt = arrow::boolean(); break;
      case 1: dt = arrow::int32(); break;
      case 2: dt = arrow::int64(); break;
      case 3: dt = arrow::float32(); break;
      case 4: dt = arrow::float64(); break;
      default: return arrow::Status::Invalid("Unknown type index");
    }
    for (int c = 0; c < count; c++) {
      fields.push_back(arrow::field("FIELD" + std::to_string(field_idx++), dt));
    }
  }
  return arrow::schema(fields);
}

arrow::Status RunBenchmark(const Config& config) {
  const std::string out_path = "bench_mark_parquet_cpp.parquet";
  remove(out_path.c_str());

  std::ofstream csv_file("memory_usage_parquet_cpp.csv");
  if (!csv_file.is_open()) return arrow::Status::IOError("Cannot create memory CSV");
  csv_file << "iter_num,memory_usage(kb)\n";
  int iter_num = 0;

  ARROW_ASSIGN_OR_RAISE(auto schema, BuildSchema(config));
  const int num_field_columns = schema->num_fields() - 3;

  csv_file << iter_num++ << "," << get_memory_usage_kb() << "\n";

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(out_path));

  parquet::WriterProperties::Builder props_builder;
  props_builder.compression(parquet::Compression::LZ4);
  props_builder.disable_dictionary();
  props_builder.enable_dictionary("TAG1");
  props_builder.enable_dictionary("TAG2");
  auto props = props_builder.build();
  auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
  std::unique_ptr<parquet::arrow::FileWriter> writer;
  ARROW_ASSIGN_OR_RAISE(
      writer,
      parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), outfile, props, arrow_props));

  double total_prepare_sec = 0, total_write_sec = 0;
  int64_t timestamp = 0;

  for (int t = 0; t < config.tablet_num; t++) {
    csv_file << iter_num++ << "," << get_memory_usage_kb() << "\n";

    auto prepare_start = std::chrono::high_resolution_clock::now();
    const int nrows = config.tag1_num * config.tag2_num * config.timestamp_per_tag;

    arrow::Int64Builder ts_builder;
    arrow::StringBuilder tag1_builder, tag2_builder;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> field_builders;
    for (int i = 3; i < schema->num_fields(); i++) {
      switch (schema->field(i)->type()->id()) {
        case arrow::Type::BOOL:
          field_builders.push_back(std::make_unique<arrow::BooleanBuilder>());
          break;
        case arrow::Type::INT32:
          field_builders.push_back(std::make_unique<arrow::Int32Builder>());
          break;
        case arrow::Type::INT64:
          field_builders.push_back(std::make_unique<arrow::Int64Builder>());
          break;
        case arrow::Type::FLOAT:
          field_builders.push_back(std::make_unique<arrow::FloatBuilder>());
          break;
        case arrow::Type::DOUBLE:
          field_builders.push_back(std::make_unique<arrow::DoubleBuilder>());
          break;
        default:
          return arrow::Status::Invalid("Unsupported field type");
      }
    }

    for (int tag1 = 0; tag1 < config.tag1_num; tag1++) {
      for (int tag2 = 0; tag2 < config.tag2_num; tag2++) {
        for (int row = 0; row < config.timestamp_per_tag; row++) {
          int64_t ts = timestamp + row;
          ARROW_RETURN_NOT_OK(ts_builder.Append(ts));
          ARROW_RETURN_NOT_OK(tag1_builder.Append("tag1_" + std::to_string(tag1)));
          ARROW_RETURN_NOT_OK(tag2_builder.Append("tag2_" + std::to_string(tag2)));
          int col = 0;
          for (size_t type_idx = 0; type_idx < config.field_type_vector.size(); type_idx++) {
            int cnt = config.field_type_vector[type_idx];
            for (int c = 0; c < cnt; c++) {
              switch (type_idx) {
                case 0:
                  ARROW_RETURN_NOT_OK(static_cast<arrow::BooleanBuilder*>(field_builders[col].get())->Append(ts % 2 == 0));
                  break;
                case 1:
                  ARROW_RETURN_NOT_OK(static_cast<arrow::Int32Builder*>(field_builders[col].get())->Append(static_cast<int32_t>(ts)));
                  break;
                case 2:
                  ARROW_RETURN_NOT_OK(static_cast<arrow::Int64Builder*>(field_builders[col].get())->Append(ts));
                  break;
                case 3:
                  ARROW_RETURN_NOT_OK(static_cast<arrow::FloatBuilder*>(field_builders[col].get())->Append(static_cast<float>(ts * 1.1)));
                  break;
                case 4:
                  ARROW_RETURN_NOT_OK(static_cast<arrow::DoubleBuilder*>(field_builders[col].get())->Append(static_cast<double>(ts) * 1.1));
                  break;
              }
              col++;
            }
          }
        }
      }
    }

    std::shared_ptr<arrow::Array> ts_arr, tag1_arr, tag2_arr;
    ARROW_ASSIGN_OR_RAISE(ts_arr, ts_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(tag1_arr, tag1_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(tag2_arr, tag2_builder.Finish());
    std::vector<std::shared_ptr<arrow::Array>> field_arrays;
    for (size_t i = 0; i < field_builders.size(); i++) {
      std::shared_ptr<arrow::Array> arr;
      ARROW_ASSIGN_OR_RAISE(arr, field_builders[i]->Finish());
      field_arrays.push_back(arr);
    }

    total_prepare_sec += std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - prepare_start).count();
    csv_file << iter_num++ << "," << get_memory_usage_kb() << "\n";

    auto write_start = std::chrono::high_resolution_clock::now();
    std::vector<std::shared_ptr<arrow::Array>> all_arrays = {ts_arr, tag1_arr, tag2_arr};
    for (auto& a : field_arrays) all_arrays.push_back(a);
    auto table = arrow::Table::Make(schema, all_arrays);
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table, nrows));
    total_write_sec += std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - write_start).count();
    timestamp += config.timestamp_per_tag;
    csv_file << iter_num++ << "," << get_memory_usage_kb() << "\n";
  }

  ARROW_RETURN_NOT_OK(writer->Close());
  csv_file << iter_num++ << "," << get_memory_usage_kb() << "\n";

  std::ifstream size_in(out_path, std::ifstream::ate | std::ifstream::binary);
  int64_t file_size = size_in.tellg();
  size_in.close();

  int64_t total_points = static_cast<int64_t>(config.tablet_num) * config.tag1_num * config.tag2_num *
                        config.timestamp_per_tag * num_field_columns;
  double writing_speed = total_points / (total_prepare_sec + total_write_sec);

  std::cout << "Finish benchmark for Parquet (C++)\n";
  std::cout << "Parquet size is " << file_size << " bytes ~ " << (file_size / 1024) << " KB\n";
  std::cout << "Prepare time is " << total_prepare_sec << " s\n";
  std::cout << "Write time is " << total_write_sec << " s\n";
  std::cout << "Writing speed is " << static_cast<int64_t>(writing_speed) << " points/s\n";

  auto read_start = std::chrono::high_resolution_clock::now();
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(out_path, arrow::default_memory_pool()));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));
  std::shared_ptr<arrow::Table> read_table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&read_table));
  int64_t row_count = read_table->num_rows();
  double read_sec = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - read_start).count();
  int64_t reading_speed = (read_sec > 0) ? static_cast<int64_t>(row_count * num_field_columns / read_sec) : 0;

  std::cout << "Row count: " << row_count << "\n";
  std::cout << "Read time is " << read_sec << " s\n";
  std::cout << "Reading speed is " << reading_speed << " points/s\n";

  auto round2 = [](double value) { return std::round(value * 100.0) / 100.0; };

  nlohmann::json result;
  result["tsfile_size"] = file_size / 1024;
  result["prepare_time"] = round2(total_prepare_sec);
  result["writing_time"] = round2(total_write_sec);
  result["writing_speed"] = round2(writing_speed);
  result["reading_time"] = round2(read_sec);
  result["reading_speed"] = round2(static_cast<double>(reading_speed));

  std::ofstream json_out("/result/results_parquet_cpp.json");
  if (json_out.is_open())
    json_out << result.dump(2);
  else
    std::cerr << "Warning: cannot write /result/results_parquet_cpp.json\n";

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  (void)argc;
  (void)argv;
  Config config = load_config("/tmp/conf.json");
  arrow::Status st = RunBenchmark(config);
  if (!st.ok()) {
    std::cerr << "Benchmark failed: " << st.ToString() << "\n";
    return 1;
  }
  return 0;
}
