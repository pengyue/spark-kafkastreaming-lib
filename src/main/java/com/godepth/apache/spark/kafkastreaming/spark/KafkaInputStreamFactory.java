package com.godepth.apache.spark.kafkastreaming.spark;

import kafka.common.TopicAndPartition;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.Serializable;
import java.util.Map;

public class KafkaInputStreamFactory implements Serializable {

    private final Map<String, String> kafkaProperties;

    public KafkaInputStreamFactory(
        Map<String, String> kafkaProperties
    ) {
        this.kafkaProperties = kafkaProperties;
    }

    public JavaInputDStream<byte[]> create(
        Map<TopicAndPartition, Long> kafkaTopicOffsets,
        JavaStreamingContext streamingContext
    ) {
        return KafkaUtils
            .createDirectStream(
                streamingContext,
                String.class,
                byte[].class,
                StringDecoder.class,
                DefaultDecoder.class,
                byte[].class,
                kafkaProperties,
                kafkaTopicOffsets,
                m -> m.message()
            );
    }
}
