package com.godepth.apache.spark.kafkastreaming.spark;

import com.godepth.apache.spark.kafkastreaming.statsd.MetricsClient;
import org.apache.commons.collections4.IterableUtils;

import java.time.Instant;

public class SparkStreamConsumerStatsdDecorator<T> implements SparkStreamConsumer<T> {

    private final SparkStreamConsumer<T> sparkStreamConsumer;
    private final MetricsClient metricsClient;

    public SparkStreamConsumerStatsdDecorator(
        SparkStreamConsumer<T> sparkStreamConsumer,
        MetricsClient metricsClient
    ) {
        this.sparkStreamConsumer = sparkStreamConsumer;
        this.metricsClient = metricsClient;
    }

    public void consume(
        String topic,
        int partition,
        Iterable<T> messages
    ) {
        int numberOfMessages = IterableUtils.size(messages);

        try {

            Instant consumptionStart = Instant.now();

            sparkStreamConsumer.consume(topic, partition, messages);

            Instant consumptionEnd = Instant.now();

            metricsClient.incrementSuccess(numberOfMessages);
            metricsClient.recordBatchTime(
                numberOfMessages,
                (consumptionEnd.toEpochMilli() - consumptionStart.toEpochMilli())
            );

            metricsClient.recordMemoryUsage(partition);

        } catch (RuntimeException e) {
            metricsClient.incrementFailure(numberOfMessages);
            throw e;
        }
    }
}
