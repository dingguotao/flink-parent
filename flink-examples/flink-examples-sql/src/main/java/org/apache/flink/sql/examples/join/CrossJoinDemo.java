package org.apache.flink.sql.examples.join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.sql.examples.SQLRunner;

import java.io.IOException;

public class CrossJoinDemo {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 4);
        SQLRunner.run("cross_join.sql", configuration);
    }
}
