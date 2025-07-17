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

package org.apache.tsfile.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tsfile.enums.TSDataType;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ConfigLoad {
    public int tablet_num;
    public int tag1_num;
    public int tag2_num;
    public int timestamp_per_tag;
    public List<Integer> field_type_vector;

    public static ConfigLoad Load(String path) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(new File(path), ConfigLoad.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public TSDataType getTSDataType(int i) {
        switch (i) {
            case 0:
                return TSDataType.BOOLEAN;
            case 1:
                return TSDataType.INT32;
            case 2:
                return TSDataType.INT64;
            case 3:
                return TSDataType.FLOAT;
            case 4:
                return TSDataType.DOUBLE;
            case 5:
                return TSDataType.BOOLEAN;
            default:
                throw new RuntimeException("Unknown TSDataType: " + i);
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "tablet_num=" + tablet_num +
                ", tag1_num=" + tag1_num +
                ", tag2_num=" + tag2_num +
                ", timestamp_per_tag=" + timestamp_per_tag +
                ", field_type_vector=" + field_type_vector +
                '}';
    }

}
