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

package org.apache.flink.sql.examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Utils to run sql scripts. */
public class SQLRunner {
    private static final Logger log = LoggerFactory.getLogger(SQLRunner.class);
    private static final String baseDir = "./flink-examples/flink-examples-sql/src/main/resources";

    public static void run(String sqlFile) throws IOException {
        run(baseDir, sqlFile);
    }

    public static void run(String sqlFile, Configuration configuration) throws IOException {
        run(baseDir, sqlFile, configuration);
    }

    public static void run(String dir, String sqlFile) throws IOException {
        run(dir, sqlFile, new Configuration());
    }

    public static void run(String dir, String sqlFile, Configuration conf) throws IOException {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        List<String> strings = FileUtils.readLines(new File(dir, sqlFile), StandardCharsets.UTF_8);
        String sql =
                strings.stream()
                        .map(String::trim)
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));

        String[] splits = sql.split(";");
        for (String split : splits) {
            try {
                tableEnv.executeSql(split);
            } catch (Exception e) {
                log.error("parse sql exception, sql context \n{}", addLineNumber(split), e);
                throw e;
            }
        }
    }

    private static String addLineNumber(String sql) {
        String[] splits = sql.split("\n");
        int i = 0;
        List<String> result = new ArrayList<>();
        for (String split : splits) {
            result.add(++i + "\t\t\t\t" + split);
        }
        return String.join("\n", result);
    }
}
