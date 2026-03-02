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
#include <fstream>
#include <nlohmann/json.hpp>

#ifdef __linux__
#include <unistd.h>
#endif
#ifdef __APPLE__
#include <mach/mach.h>
#endif

Config load_config(const std::string& config_path) {
  std::ifstream f(config_path);
  nlohmann::json j;
  f >> j;
  Config c;
  c.tablet_num = j["tablet_num"].get<int>();
  c.tag1_num = j["tag1_num"].get<int>();
  c.tag2_num = j["tag2_num"].get<int>();
  c.timestamp_per_tag = j["timestamp_per_tag"].get<int>();
  c.field_type_vector = j["field_type_vector"].get<std::vector<int>>();
  return c;
}

int get_memory_usage_kb() {
#ifdef __linux__
  std::ifstream status_file("/proc/self/status");
  std::string line;
  while (std::getline(status_file, line)) {
    if (line.find("VmRSS") == 0) {
      size_t num_pos = line.find_first_of("0123456789");
      if (num_pos != std::string::npos) {
        size_t end_pos = line.find_first_not_of("0123456789", num_pos);
        std::string num_str = line.substr(num_pos, end_pos - num_pos);
        return static_cast<int>(strtoul(num_str.c_str(), nullptr, 10));
      }
    }
  }
#elif defined(__APPLE__)
  task_basic_info_data_t info;
  mach_msg_type_number_t count = TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&info, &count) == KERN_SUCCESS) {
    return static_cast<int>(info.resident_size / 1024);
  }
  return 0;
#endif
  return 0;
}
