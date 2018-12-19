package com.godepth.apache.spark.kafkastreaming.spark;

import java.io.Serializable;

public class KafkaOffsetValidator implements Serializable {

    private final KafkaOffsetReader kafkaOffsetReader;

    public KafkaOffsetValidator(
        KafkaOffsetReader kafkaOffsetReader
    ) {
        this.kafkaOffsetReader = kafkaOffsetReader;
    }

    public void validateOffset(
        KafkaOffsetRange kafkaOffsetRange
    ) {
        int partition = kafkaOffsetRange.getPartition();

        long previousOffset = kafkaOffsetReader.readOffset(partition);
        long startOffset = kafkaOffsetRange.getFromOffset();

        if (previousOffset < startOffset) {

            throw new RuntimeException(
                "Previously stored kafka offset for partition " + partition + " "
                + "is less than the start offset - messages would have been skipped "
                + "(" + previousOffset + " < " + startOffset + ")"
            );
        }
    }
}
