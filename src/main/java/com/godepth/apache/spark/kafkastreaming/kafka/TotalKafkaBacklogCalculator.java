package com.godepth.apache.spark.kafkastreaming.kafka;

import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetRange;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;

public class TotalKafkaBacklogCalculator implements BacklogCalculator, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TotalKafkaBacklogCalculator.class);

    private final KafkaOffsetReader kafkaOffsetReader;
    private final TotalKafkaOffsetRangeFetcher totalKafkaOffsetRangeFetcher;

    public TotalKafkaBacklogCalculator(
        KafkaOffsetReader kafkaOffsetReader,
        TotalKafkaOffsetRangeFetcher totalKafkaOffsetRangeFetcher
    ) {
        this.kafkaOffsetReader = kafkaOffsetReader;
        this.totalKafkaOffsetRangeFetcher = totalKafkaOffsetRangeFetcher;
    }

    public Optional<Long> calculate(
        String topic,
        int partition
    ) {
        try {

            Optional<KafkaOffsetRange> totalKafkaOffsetRange =
                totalKafkaOffsetRangeFetcher
                    .fetch(topic, partition);

            if (!totalKafkaOffsetRange.isPresent()) {
                return Optional.empty();
            }

            long lastCommittedOffset =
                kafkaOffsetReader
                    .readOffset(partition);

            long maximumOffsetPossible = totalKafkaOffsetRange.get().getUntilOffset();

            long backlog = maximumOffsetPossible - lastCommittedOffset;

            return Optional.of(backlog);

        } catch (RuntimeException e) {

            LOGGER.warn(
                "@@ Backlog for " + topic + ":" + partition +
                " could not be calculated",
                e
            );

            return Optional.empty();
        }
    }
}
