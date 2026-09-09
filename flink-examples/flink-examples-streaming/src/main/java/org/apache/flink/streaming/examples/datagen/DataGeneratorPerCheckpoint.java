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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.FsStateChangelogOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Random;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTING_INTERVAL;

/**
 * An example for generating specific data per checkpoint with a {@link DataGeneratorSource} .
 */
public class DataGeneratorPerCheckpoint {

    public static void main(String[] args) throws Exception {

        String checkpointBaseDir = "file:///Users/clouding/checkpoints/datagen";

        Configuration conf = new Configuration();

        // checkpoint setting
        conf.set(CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        conf.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointBaseDir);

        // changelog setting
        conf.set(StateChangelogOptions.STATE_CHANGE_LOG_STORAGE, "filesystem");
        conf.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        conf.set(StateChangelogOptions.PERIODIC_MATERIALIZATION_ENABLED, true);
        conf.set(StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL, Duration.ofMinutes(1));

        // dstl setting
        conf.set(FsStateChangelogOptions.BASE_PATH, checkpointBaseDir);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);

        final String[] elements = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        final int size = elements.length;
        final GeneratorFunction<Long, String> generatorFunction =
                index -> elements[(int) (index % size)] +  index % 100;

        final DataGeneratorSource<String> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perCheckpoint(10000),
                        Types.STRING);

        final DataStreamSource<String> streamSource =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = streamSource.map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .reduce((tuple1, tuple2) -> new Tuple2<>(tuple1.f0, tuple1.f1 + tuple2.f1));
        reduce.executeAndCollect("Data Generator Source Example");

//        env.execute("Data Generator Source Example");
    }
}
