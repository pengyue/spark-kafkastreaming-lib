package com.godepth.apache.spark.kafkastreaming.spark;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

public interface JavaStreamingContextFactory {

    JavaStreamingContext create();
}
