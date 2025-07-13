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

#ifndef TSFILE_BENCH_MARK_BENCH_MARK_UTILS_H
#define TSFILE_BENCH_MARK_BENCH_MARK_UTILS_H
#include <vector>

static const char* data_types_name[5] = {"BOOLEAN", "INT32", "INT64", "FLOAT",
                                         "DOUBLE"};
static const int data_type[5] = {0, 1, 2, 3, 4};

struct Config {
  int tablet_num;
  int tag1_num;
  int tag2_num;
  int timestamp_per_tag;
  std::vector<int> field_type_vector;
};
Config load_config(const std::string& config_path);
void print_config(bool is_cpp, Config config);
void print_progress_bar(int current, int total, int barWidth = 50);
int get_memory_usage();


#endif  // TSFILE_BENCH_MARK_BENCH_MARK_UTILS_H