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
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetBenchMark {

    public static void main(String[] args) throws Exception {
        ConfigLoad config = ConfigLoad.Load("/tmp/conf.json");
        MemoryMonitor monitor = new MemoryMonitor();

        String outPath = "bench_mark_parquet_java.parquet";
        File f = new File(outPath);
        if (f.exists()) f.delete();

        Schema schema = buildAvroSchema(config);
        List<String> fieldNames = buildFieldNames(config);
        int numFieldColumns = fieldNames.size() - 3; // exclude timestamp, TAG1, TAG2

        monitor.recordMemoryUsage();

        long totalPrepareNs = 0;
        long totalWriteNs = 0;
        long timestamp = 0;

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(outPath))
                .withSchema(schema)
                .withConf(new Configuration())
                .build()) {

            monitor.recordMemoryUsage();

            for (int t = 0; t < config.tablet_num; t++) {
                long prepareStart = System.nanoTime();
                List<GenericRecord> records = new ArrayList<>(
                        config.tag1_num * config.tag2_num * config.timestamp_per_tag);

                for (int tag1 = 0; tag1 < config.tag1_num; tag1++) {
                    for (int tag2 = 0; tag2 < config.tag2_num; tag2++) {
                        for (int row = 0; row < config.timestamp_per_tag; row++) {
                            GenericRecord rec = new GenericData.Record(schema);
                            rec.put("timestamp", timestamp + row);
                            rec.put("TAG1", "tag1_" + tag1);
                            rec.put("TAG2", "tag2_" + tag2);
                            int colIdx = 3;
                            for (int typeIdx = 0; typeIdx < config.field_type_vector.size(); typeIdx++) {
                                int count = config.field_type_vector.get(typeIdx);
                                String avroType = config.getAvroType(typeIdx);
                                long ts = timestamp + row;
                                for (int c = 0; c < count; c++) {
                                    String name = fieldNames.get(colIdx);
                                    switch (avroType) {
                                        case "boolean":
                                            rec.put(name, (ts % 2) == 0);
                                            break;
                                        case "int":
                                            rec.put(name, (int) ts);
                                            break;
                                        case "long":
                                            rec.put(name, ts);
                                            break;
                                        case "float":
                                            rec.put(name, (float) (ts * 1.1));
                                            break;
                                        case "double":
                                            rec.put(name, (double) ts * 1.1);
                                            break;
                                        default:
                                            throw new RuntimeException("Unknown: " + avroType);
                                    }
                                    colIdx++;
                                }
                            }
                            records.add(rec);
                        }
                    }
                }
                totalPrepareNs += System.nanoTime() - prepareStart;
                monitor.recordMemoryUsage();

                long writeStart = System.nanoTime();
                for (GenericRecord rec : records) {
                    writer.write(rec);
                }
                totalWriteNs += System.nanoTime() - writeStart;
                monitor.recordMemoryUsage();
                timestamp += config.timestamp_per_tag;
            }
        }

        long size = new File(outPath).length();
        monitor.recordMemoryUsage();
        monitor.close();

        double prepareSec = totalPrepareNs / 1_000_000_000.0;
        double writeSec = totalWriteNs / 1_000_000_000.0;
        long totalPoints = (long) config.tablet_num * config.tag1_num * config.tag2_num
                * config.timestamp_per_tag * numFieldColumns;
        double writingSpeed = totalPoints / (prepareSec + writeSec);

        System.out.println("Finish benchmark for Parquet (Java)");
        System.out.printf("Parquet size is %d bytes ~ %d KB%n", size, size / 1024);
        System.out.printf("Prepare time is %.6f s%n", prepareSec);
        System.out.printf("Write time is %.6f s%n", writeSec);
        System.out.printf("Writing speed is %d points/s%n", (long) writingSpeed);

        long readStart = System.nanoTime();
        long rowCount = 0;
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(outPath)).build()) {
            GenericRecord r;
            while ((r = reader.read()) != null) rowCount++;
        }
        double readSec = (System.nanoTime() - readStart) / 1_000_000_000.0;
        long readPoints = rowCount * numFieldColumns;
        long readingSpeed = readSec > 0 ? (long) (readPoints / readSec) : 0;

        System.out.println("Row count: " + rowCount);
        System.out.printf("Read time is %.6f s%n", readSec);
        System.out.printf("Reading speed is %d points/s%n", readingSpeed);

        Map<String, Object> result = new HashMap<>();
        result.put("tsfile_size", size / 1024);
        result.put("prepare_time", prepareSec);
        result.put("write_time", writeSec);
        result.put("writing_speed", writingSpeed);
        result.put("reading_time", readSec);
        result.put("reading_speed", readingSpeed);

        new ObjectMapper().writerWithDefaultPrettyPrinter()
                .writeValue(new File("/result/results_parquet_java.json"), result);
    }

    private static List<String> buildFieldNames(ConfigLoad config) {
        List<String> names = new ArrayList<>();
        names.add("timestamp");
        names.add("TAG1");
        names.add("TAG2");
        int idx = 2;
        for (int i = 0; i < config.field_type_vector.size(); i++) {
            int count = config.field_type_vector.get(i);
            for (int j = 0; j < count; j++) {
                names.add("FIELD" + (idx++));
            }
        }
        return names;
    }

    private static Schema buildAvroSchema(ConfigLoad config) {
        SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.record("BenchRecord").fields()
                .requiredLong("timestamp")
                .requiredString("TAG1")
                .requiredString("TAG2");
        int fieldIdx = 2;
        for (int typeIdx = 0; typeIdx < config.field_type_vector.size(); typeIdx++) {
            int count = config.field_type_vector.get(typeIdx);
            String avroType = config.getAvroType(typeIdx);
            for (int c = 0; c < count; c++) {
                String name = "FIELD" + (fieldIdx++);
                switch (avroType) {
                    case "boolean": fa = fa.requiredBoolean(name); break;
                    case "int":     fa = fa.requiredInt(name); break;
                    case "long":    fa = fa.requiredLong(name); break;
                    case "float":   fa = fa.requiredFloat(name); break;
                    case "double":  fa = fa.requiredDouble(name); break;
                    default: throw new RuntimeException("Unknown: " + avroType);
                }
            }
        }
        return fa.endRecord();
    }
}
