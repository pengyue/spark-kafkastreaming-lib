package com.godepth.apache.spark.kafkastreaming;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IterableToStream {

    public static <T> Stream<T> stream(
        Iterable<T> iterable
    ) {
        return StreamSupport.stream(
            iterable.spliterator(),
            false
        );
    }
}
