package com.godepth.apache.spark.kafkastreaming.spark;

import kafka.common.TopicAndPartition;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Map;

public class KafkaStreamingContextFactory<T> implements JavaStreamingContextFactory {

    private final int batchDurationInSeconds;

    private final JavaSparkContextFactory javaSparkContextFactory;
    private final KafkaInputStreamConsumer<T> kafkaInputStreamConsumer;
    private final KafkaInputStreamFactory kafkaInputStreamFactory;
    private final KafkaOffsetProvider kafkaOffsetProvider;
    private final KafkaOffsetValidatorProvider kafkaOffsetValidatorProvider;
    private final KafkaOffsetWriterProvider kafkaOffsetWriterProvider;
    private final SparkStreamConsumerProvider<T> sparkStreamConsumerProvider;
    private final SparkStreamConsumerTranscoder<T> sparkStreamConsumerTranscoder;

    public KafkaStreamingContextFactory(
        int batchDurationInSeconds,
        JavaSparkContextFactory javaSparkContextFactory,
        KafkaInputStreamConsumer<T> kafkaInputStreamConsumer,
        KafkaInputStreamFactory kafkaInputStreamFactory,
        KafkaOffsetProvider kafkaOffsetProvider,
        KafkaOffsetValidator kafkaOffsetValidator,
        KafkaOffsetWriter kafkaOffsetWriter,
        SparkStreamConsumerProvider<T> sparkStreamConsumerProvider,
        SparkStreamConsumerTranscoder<T> sparkStreamConsumerTranscoder
    ) {
        this(
            batchDurationInSeconds,
            javaSparkContextFactory,
            kafkaInputStreamConsumer,
            kafkaInputStreamFactory,
            kafkaOffsetProvider,
            () -> kafkaOffsetValidator,
            () -> kafkaOffsetWriter,
            sparkStreamConsumerProvider,
            sparkStreamConsumerTranscoder
        );
    }


    public KafkaStreamingContextFactory(
        int batchDurationInSeconds,
        JavaSparkContextFactory javaSparkContextFactory,
        KafkaInputStreamConsumer<T> kafkaInputStreamConsumer,
        KafkaInputStreamFactory kafkaInputStreamFactory,
        KafkaOffsetProvider kafkaOffsetProvider,
        KafkaOffsetValidatorProvider kafkaOffsetValidatorProvider,
        KafkaOffsetWriterProvider kafkaOffsetWriterProvider,
        SparkStreamConsumerProvider<T> sparkStreamConsumerProvider,
        SparkStreamConsumerTranscoder<T> sparkStreamConsumerTranscoder
    ) {
        this.batchDurationInSeconds = batchDurationInSeconds;
        this.javaSparkContextFactory = javaSparkContextFactory;
        this.kafkaInputStreamConsumer = kafkaInputStreamConsumer;
        this.kafkaInputStreamFactory = kafkaInputStreamFactory;
        this.kafkaOffsetProvider = kafkaOffsetProvider;
        this.kafkaOffsetValidatorProvider = kafkaOffsetValidatorProvider;
        this.kafkaOffsetWriterProvider = kafkaOffsetWriterProvider;
        this.sparkStreamConsumerProvider = sparkStreamConsumerProvider;
        this.sparkStreamConsumerTranscoder = sparkStreamConsumerTranscoder;
    }

    public JavaStreamingContext create() {

        Map<TopicAndPartition, Long> kafkaTopicOffsets = kafkaOffsetProvider.getOffsets();

        JavaSparkContext javaSparkContext = javaSparkContextFactory.create();

        JavaStreamingContext streamingContext =
            new JavaStreamingContext(
                javaSparkContext,
                Durations.seconds(batchDurationInSeconds)
            );

        JavaInputDStream<byte[]> stream =
            kafkaInputStreamFactory
                .create(
                    kafkaTopicOffsets,
                    streamingContext
                );

        kafkaInputStreamConsumer
            .consume(
                kafkaOffsetValidatorProvider,
                kafkaOffsetWriterProvider,
                sparkStreamConsumerProvider,
                sparkStreamConsumerTranscoder,
                stream
            );

        return streamingContext;
    }
}
