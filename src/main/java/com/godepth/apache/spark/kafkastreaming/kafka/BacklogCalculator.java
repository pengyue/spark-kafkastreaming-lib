package com.godepth.apache.spark.kafkastreaming.kafka;

import java.io.Serializable;
import java.util.Optional;

public interface BacklogCalculator extends Serializable {

    Optional<Long> calculate(
        String topic,
        int partition
    );
}
