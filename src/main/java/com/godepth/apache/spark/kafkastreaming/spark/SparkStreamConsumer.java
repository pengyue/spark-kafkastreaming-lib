package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public interface SparkStreamConsumer<T> extends Serializable {

    void consume(
        String topic,
        int partition,
        Iterable<T> messages
    );
}
