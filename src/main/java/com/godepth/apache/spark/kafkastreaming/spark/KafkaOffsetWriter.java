package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public interface KafkaOffsetWriter extends Serializable {

    void writeOffset(
        KafkaOffsetRange kafkaOffsetRange
    );
}
