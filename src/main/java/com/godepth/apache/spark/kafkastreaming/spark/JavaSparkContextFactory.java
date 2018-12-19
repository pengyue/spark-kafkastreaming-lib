package com.godepth.apache.spark.kafkastreaming.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Map;

public class JavaSparkContextFactory implements Serializable {

    private final Map<String, String> sparkProperties;

    public JavaSparkContextFactory(
        Map<String, String> sparkProperties
    ) {
        this.sparkProperties = sparkProperties;
    }

    public JavaSparkContext create() {

        SparkConf conf = new SparkConf();

        sparkProperties
            .entrySet()
            .forEach(
                sparkProperty -> {
                    if (sparkProperty.getValue() != null) {
                        conf.set(
                            sparkProperty.getKey(),
                            sparkProperty.getValue()
                        );
                    }
                }
            );

        return new JavaSparkContext(conf);
    }
}
