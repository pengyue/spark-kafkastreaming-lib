package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public class KafkaPartitionState implements Serializable {

    private final KafkaOffsetRange kafkaOffsetRange;

    public KafkaPartitionState(
        KafkaOffsetRange kafkaOffsetRange
    ) {
        this.kafkaOffsetRange = kafkaOffsetRange;
    }

    public KafkaOffsetRange getKafkaOffsetRange() {
        return kafkaOffsetRange;
    }
}
