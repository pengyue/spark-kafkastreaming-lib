package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public interface KafkaOffsetWriterProvider extends Serializable {

    KafkaOffsetWriter provide();
}
