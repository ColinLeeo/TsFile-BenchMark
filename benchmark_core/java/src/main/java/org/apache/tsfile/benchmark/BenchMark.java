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
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import org.apache.tsfile.benchmark.ConfigLoad;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchMark {
    private static final Logger LOGGER = LoggerFactory.getLogger(BenchMark.class);

    public static void main(String[] args) throws Exception {
        ConfigLoad configLoad = ConfigLoad.Load("tmp/json");
        System.out.println(configLoad);

        MemoryMonitor monitor = new MemoryMonitor();
        String path = "tsfile_table_write_bench_mark_java.tsfile";
        File f = FSFactoryProducer.getFSFactory().getFile(path);
        if (f.exists()) {
            Files.delete(f.toPath());
        }
        monitor.recordMemoryUsage();
        List<String> column_names = new ArrayList<>();
        List<TSDataType> column_types = new ArrayList<>();
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(new ColumnSchemaBuilder().name("TAG1").dataType(TSDataType.STRING).category(Tablet.ColumnCategory.TAG).build());
        columnSchemas.add(new ColumnSchemaBuilder().name("TAG2").dataType(TSDataType.STRING).category(Tablet.ColumnCategory.TAG).build());
        column_names.add("TAG1");
        column_names.add("TAG2");
        column_types.add(TSDataType.STRING);
        column_types.add(TSDataType.STRING);

        int fieldIndex = 2;
        for (int i = 0; i < configLoad.field_type_vector.size(); i++) {
            int count = configLoad.field_type_vector.get(i);
            TSDataType dataType = configLoad.getTSDataType(i);
            for (int j = 0; j < count; j++) {
                columnSchemas.add(new ColumnSchemaBuilder().name("FIELD" + fieldIndex).dataType(dataType).category(Tablet.ColumnCategory.FIELD).build());
                column_names.add("FIELD" + fieldIndex);
                column_types.add(dataType);
                fieldIndex++;
            }
        }
        monitor.recordMemoryUsage();
        long totalPrepareTimeNs = 0;
        long totalWriteTimeNs = 0;
        long writeStartTime = 0;
        TableSchema tableSchema = new TableSchema("TestTable", columnSchemas);
        monitor.recordMemoryUsage();
        try (ITsFileWriter writer = new TsFileWriterBuilder().file(f).tableSchema(tableSchema).build()) {
            monitor.recordMemoryUsage();
            long timestamp = 0;
            for (int table_ind = 0; table_ind < configLoad.tablet_num; table_ind++) {
                long prepareStartTime = System.nanoTime();
                Tablet tablet = new Tablet(column_names, column_types, configLoad.tag1_num * configLoad.tag2_num * configLoad.timestamp_per_tag);
                int row_count = 0;
                for (int tag1_ind = 0; tag1_ind < configLoad.tag1_num; tag1_ind++) {
                    for (int tag2_ind = 0; tag2_ind < configLoad.tag2_num; tag2_ind++) {
                        for (int row = 0; row < configLoad.timestamp_per_tag; row++) {
                            tablet.addTimestamp(row_count, timestamp + row);
                            tablet.addValue(row_count, 0, "tag1_" + tag1_ind);
                            tablet.addValue(row_count, 1, "tag2_" + tag2_ind);

                            for (int i = 2; i < column_types.size(); i++) {
                                switch (column_types.get(i)) {
                                    case INT32:
                                        tablet.addValue(row_count, i, (int) timestamp);
                                        break;
                                    case INT64:
                                        tablet.addValue(row_count, i, timestamp);
                                        break;
                                    case FLOAT:
                                        tablet.addValue(row_count, i, (float) (timestamp * 1.1));
                                        break;
                                    case DOUBLE:
                                        tablet.addValue(row_count, i, (double) timestamp * 1.1);
                                        break;
                                    case BOOLEAN:
                                        tablet.addValue(row_count, i, timestamp % 2 == 0);
                                    default:
                                        //
                                }
                            }
                            row_count++;
                        }
                    }
                }
                monitor.recordMemoryUsage();
                long prepareEndTime = System.nanoTime();

                totalPrepareTimeNs += (prepareEndTime - prepareStartTime);
                writeStartTime = System.nanoTime();
                writer.write(tablet);
                monitor.recordMemoryUsage();
                long writeEndTime = System.nanoTime();
                totalWriteTimeNs += (writeEndTime - writeStartTime);
                timestamp += configLoad.timestamp_per_tag;
            }
            writeStartTime = System.nanoTime();
        } catch (WriteProcessException e) {
            LOGGER.error("meet error in TsFileWrite ", e);
        }
        long writeEndTime = System.nanoTime();
        totalWriteTimeNs += (writeEndTime - writeStartTime);
        monitor.recordMemoryUsage();
        long end = System.nanoTime();
        double totalPrepareTimeSec = totalPrepareTimeNs / 1_000_000_000.0;
        double totalWriteTimeSec = totalWriteTimeNs / 1_000_000_000.0;
        long size = f.length();

        monitor.close();
        System.out.println("Finish bench mark for java");
        System.out.printf("Tsfile size is %d bytes ~ %dKB%n", size, size / 1024);

        System.out.printf("Prepare data time is %.6f s%n", totalPrepareTimeSec);
        System.out.printf("Writing data time is %.6f s%n", totalWriteTimeSec);

        long totalPoints = (long) configLoad.tablet_num * configLoad.tag1_num * configLoad.tag2_num * configLoad.timestamp_per_tag * (column_names.size() - 2);
        double writingSpeed = totalPoints / (totalPrepareTimeSec + totalWriteTimeSec);
        System.out.printf("writing speed is %d points/s%n", (long) writingSpeed);
        Map<String, Object> result = new HashMap<>();
        result.put("tsfile_size", size / 1024);
        result.put("prepare_time", totalPrepareTimeSec);
        result.put("write_time", totalWriteTimeSec);
        result.put("writing_speed", writingSpeed);


        Integer row = 0;
        long read_start = System.nanoTime();
        try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build()) {
            ResultSet resultSet = reader.query("TestTable", column_names, Long.MIN_VALUE, Long.MAX_VALUE);
            while (resultSet.next()) {
                row++;
            }
        }
        System.out.println("row is " + row);
        long read_end = System.nanoTime();
        double totalReadTimeSec = (read_end - read_start) / 1_000_000_000.0;
        System.out.printf("read time is %.6f s%n", totalReadTimeSec);
        Long readSpeed = Math.round(row * column_names.size() / totalReadTimeSec);
        System.out.printf("read speed is %.6f points/s %n", readSpeed);
        result.put("reading_time", totalReadTimeSec);
        result.put("reading_speed", readSpeed);
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File("/result/results_java.json"), result);
    }
}
