package com.godepth.apache.spark.kafkastreaming.spark;

import kafka.common.TopicAndPartition;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class KafkaOffsetProvider implements Serializable {

    private final KafkaOffsetReader kafkaOffsetReader;
    private final String topic;
    private final int numberOfPartitions;

    public KafkaOffsetProvider(
        KafkaOffsetReader kafkaOffsetReader,
        String topic,
        int numberOfPartitions
    ) {
        this.kafkaOffsetReader = kafkaOffsetReader;
        this.topic = topic;
        this.numberOfPartitions = numberOfPartitions;
    }

    public Map<TopicAndPartition, Long> getOffsets() {

        Map<TopicAndPartition, Long> offsets = new HashMap<>();

        for (int i = 0; i < numberOfPartitions; i++) {
            offsets.put(
                new TopicAndPartition(
                    topic,
                    i
                ),
                kafkaOffsetReader.readOffset(i)
            );
        }

        return offsets;
    }
}
