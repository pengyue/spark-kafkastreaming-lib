package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public interface SparkStreamConsumerTranscoder<T> extends Serializable {

    Iterable<T> transcode(
        Iterable<byte[]> messages
    );
}
