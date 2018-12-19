package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public interface SparkStreamConsumerProvider<T> extends Serializable {

    SparkStreamConsumer<T> provide();
}
