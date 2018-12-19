package com.godepth.apache.spark.kafkastreaming.spark;

import com.godepth.apache.spark.kafkastreaming.IterableToStream;

import java.util.stream.Collectors;

public class SparkStreamConsumerBytesToStringTranscoder implements SparkStreamConsumerTranscoder<String> {

    public Iterable<String> transcode(
        Iterable<byte[]> messages
    ) {
        return
            IterableToStream
                .stream(messages)
                .map(message -> new String(message))
                .collect(Collectors.toList());
    }
}
