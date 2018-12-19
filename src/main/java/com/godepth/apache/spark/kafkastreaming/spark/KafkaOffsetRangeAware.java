package com.godepth.apache.spark.kafkastreaming.spark;

public interface KafkaOffsetRangeAware {

    void setKafkaOffsetRange(
        KafkaOffsetRange kafkaOffsetRange
    );
}
