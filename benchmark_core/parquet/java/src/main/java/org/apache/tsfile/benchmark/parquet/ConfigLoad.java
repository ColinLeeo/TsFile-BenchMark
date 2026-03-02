/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package org.apache.tsfile.benchmark.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ConfigLoad {
    public int tablet_num;
    public int tag1_num;
    public int tag2_num;
    public int timestamp_per_tag;
    public List<Integer> field_type_vector;

    public static ConfigLoad Load(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(path), ConfigLoad.class);
    }

    /** 0=BOOLEAN, 1=INT32, 2=INT64, 3=FLOAT, 4=DOUBLE (same as TsFile ConfigLoad) */
    public String getAvroType(int typeIndex) {
        switch (typeIndex) {
            case 0: return "boolean";
            case 1: return "int";
            case 2: return "long";
            case 3: return "float";
            case 4: return "double";
            default: throw new RuntimeException("Unknown type index: " + typeIndex);
        }
    }
}
