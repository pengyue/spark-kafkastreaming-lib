package com.godepth.apache.spark.kafkastreaming.spark;

public class SparkStreamConsumerBytesPassthroughTranscoder implements SparkStreamConsumerTranscoder<byte[]> {

    public Iterable<byte[]> transcode(
        Iterable<byte[]> messages
    ) {
        return messages;
    }
}
