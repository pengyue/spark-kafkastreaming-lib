package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public interface KafkaOffsetValidatorProvider extends Serializable {

    KafkaOffsetValidator provide();
}
