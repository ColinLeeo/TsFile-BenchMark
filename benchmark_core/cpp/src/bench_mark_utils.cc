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

#include "bench_mark_utils.h"

#include <sys/resource.h>
#include <nlohmann/json.hpp>


#include <fstream>
#include <iostream>
#include <string>

#ifdef __APPLE__
#include <mach/mach_init.h>
#include <mach/message.h>
#include <mach/task.h>
#include <mach/task_info.h>
#endif

Config load_config(const std::string& config_path) {
    std::ifstream config_file(config_path);
    nlohmann::json json;
    config_file >> json;

    Config config;
    config.tablet_num = json["tablet_num"].get<int>();
    config.tag1_num = json["tag1_num"].get<int>();
    config.tag2_num = json["tag2_num"].get<int>();
    config.timestamp_per_tag = json["timestamp_per_tag"].get<int>();
    config.field_type_vector = json["field_type_vector"].get<std::vector<int>>();
    return config;
}




void print_config(bool is_cpp, Config config) {
    std::cout << "====================" << std::endl;
    std::cout << "TsFile benchmark For CPP" << std::endl;
    std::cout << "Schema Configuration:" << std::endl;
    std::cout << "TAG1 num: " << config.tag1_num << ", TAG2 num: " << config.tag2_num
              << std::endl;
    std::cout << "Filed columns (type x num) :  " << std::endl;
    int column_num = 0;
    for (int i = 0; i < 5; i++) {
        std::cout << data_types_name[i] << "x" << config.field_type_vector[i] << " ";
        column_num += config.field_type_vector[i];
    }

    std::cout << std::endl;
    std::cout << "Tablet num:" << config.tablet_num << std::endl;
    std::cout << "Tablet row num per tag:" << config.timestamp_per_tag << std::endl;
    std::cout << "Total points is "
              << config.tablet_num * config.tag1_num * config.tag2_num * config.timestamp_per_tag *
                     (column_num)
              << std::endl;
}

void print_progress_bar(int current, int total, int barWidth) {
    float progress = static_cast<float>(current) / total;
    int pos = barWidth * progress;

    std::cout << "[";
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos)
            std::cout << "=";
        else if (i == pos)
            std::cout << ">";
        else
            std::cout << " ";
    }
    std::cout << "] " << int(progress * 100.0) << " %\r";
    std::cout.flush();
}

int get_memory_usage() {
#ifdef _WIN32
#include <window.h>
#include <psapi.h>
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
        return pmc.WorkingSetSize / 1024 ;
    } else {
        return 0;
    }
#elif defined(__linux__)
    std::ifstream status_file("/proc/self/status");
    std::string line;
    while (std::getline(status_file, line)) {
        if (line.find("VmRSS") == 0) {
            size_t num_pos = line.find_first_of("0123456789");
            if (num_pos != std::string::npos) {
                size_t end_pos = line.find_first_not_of("1234567890", num_pos);
                std::string num_str = line.substr(num_pos, end_pos - num_pos);

                unsigned long vm_rss_kb = strtoul(num_str.c_str(), nullptr, 10);
                return vm_rss_kb;
            }
        }
    }
#elif defined(__APPLE__)

    task_basic_info_data_t info;
    mach_msg_type_number_t count = TASK_BASIC_INFO_COUNT;
    kern_return_t ret = task_info(
        mach_task_self(),
        TASK_BASIC_INFO,
        (task_info_t)&info,
        &count
    );
    if (ret == KERN_SUCCESS) {
        return info.resident_size / 1024 ;
    } else {
        return 0;
    }
#endif
}
