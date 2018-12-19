package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public interface KafkaOffsetReader extends Serializable {

    long readOffset(
        int partition
    );
}
