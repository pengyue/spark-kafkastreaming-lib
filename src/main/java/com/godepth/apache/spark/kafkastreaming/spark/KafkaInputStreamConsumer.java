package com.godepth.apache.spark.kafkastreaming.spark;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.iterators.SingletonIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class KafkaInputStreamConsumer<T> implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaInputStreamConsumer.class);

    public final void consume(
        KafkaOffsetValidatorProvider kafkaOffsetValidatorProvider,
        KafkaOffsetWriterProvider kafkaOffsetWriterProvider,
        SparkStreamConsumerProvider<T> sparkStreamConsumerProvider,
        SparkStreamConsumerTranscoder<T> sparkStreamConsumerTranscoder,
        JavaInputDStream<byte[]> stream
    ) {
        stream.foreachRDD( // kafka topic
            (Function<JavaRDD<byte[]>, Void>) rdd -> { // runs on driver

                String batchIdentifier =
                    Long.toHexString(Double.doubleToLongBits(Math.random()));

                LOGGER.info("@@ [" + batchIdentifier + "] Starting batch ...");

                Instant batchStart = Instant.now();

                List<KafkaPartitionState> partitionStates =
                    rdd.mapPartitionsWithIndex( // kafka partition
                        (index, messageIterator) -> { // runs on worker

                            if (!messageIterator.hasNext()) {
                                return Collections.emptyIterator();
                            }

                            Logger logger = LoggerFactory.getLogger(KafkaInputStreamConsumer.class);

                            KafkaOffsetValidator kafkaOffsetValidator = kafkaOffsetValidatorProvider.provide();
                            KafkaOffsetWriter kafkaOffsetWriter = kafkaOffsetWriterProvider.provide();

                            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                            KafkaOffsetRange kafkaOffsetRange = new KafkaOffsetRange(offsetRanges[index]);
                            kafkaOffsetValidator.validateOffset(kafkaOffsetRange);

                            logger.info(
                                "@@ Consuming " + kafkaOffsetRange.getCount() +
                                " messages from topic " + kafkaOffsetRange.getTopic() +
                                " within partition " + kafkaOffsetRange.getPartition()
                            );

                            List<byte[]> messages = IteratorUtils.toList(messageIterator);

                            SparkStreamConsumer<T> sparkStreamConsumer =
                                sparkStreamConsumerProvider
                                    .provide();

                            if (sparkStreamConsumer instanceof KafkaOffsetRangeAware) {

                                ((KafkaOffsetRangeAware) sparkStreamConsumer)
                                    .setKafkaOffsetRange(kafkaOffsetRange);
                            }

                            sparkStreamConsumer.consume(
                                kafkaOffsetRange.getTopic(),
                                kafkaOffsetRange.getPartition(),
                                sparkStreamConsumerTranscoder.transcode(messages)
                            );

                            kafkaOffsetWriter.writeOffset(kafkaOffsetRange);

                            logger.info(
                                "@@ Processed " + kafkaOffsetRange.getCount() +
                                " messages from topic " + kafkaOffsetRange.getTopic() +
                                " within partition " + kafkaOffsetRange.getPartition()
                            );

                            return new SingletonIterator<>(
                                new KafkaPartitionState(kafkaOffsetRange)
                            );
                        },
                        true
                    ).collect();

                Long totalRateCount =
                    partitionStates
                        .stream()
                        .map(partitionState ->
                            partitionState
                                .getKafkaOffsetRange()
                                .getCount()
                        )
                        .mapToLong(Long::longValue)
                        .sum();

                LOGGER.info(
                    "@@ [" + batchIdentifier + "] Finished batch of " + totalRateCount + " messages " +
                    "in " + (Instant.now().toEpochMilli() - batchStart.toEpochMilli()) + "ms"
                );

                return null;
            }
        );
    }
}
