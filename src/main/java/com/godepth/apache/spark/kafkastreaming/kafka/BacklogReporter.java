package com.godepth.apache.spark.kafkastreaming.kafka;

public interface BacklogReporter {

    void report(
        String topic,
        int partition,
        long backlog
    );
}
