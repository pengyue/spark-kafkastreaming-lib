package com.godepth.apache.spark.kafkastreaming.spark;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Optional;

public class JavaStreamingContextProvider {

    private final JavaStreamingContextFactory javaStreamingContextFactory;

    private final Optional<String> optionalCheckpointPath;

    public JavaStreamingContextProvider(
        JavaStreamingContextFactory javaStreamingContextFactory,
        String checkpointPath
    ) {
        this.javaStreamingContextFactory = javaStreamingContextFactory;
        this.optionalCheckpointPath = checkpointPath == null || checkpointPath.isEmpty() ?
            Optional.empty() :
            Optional.of(checkpointPath);
    }

    /* required by Spring DI if no checkpoint path is specified */
    public JavaStreamingContextProvider(
        JavaStreamingContextFactory javaStreamingContextFactory
    ) {
        this.javaStreamingContextFactory = javaStreamingContextFactory;
        this.optionalCheckpointPath = Optional.empty();
    }

    public JavaStreamingContext getOrCreate() {

        if (optionalCheckpointPath.isPresent()) {
            return getOrCreateWithCheckpointing();
        }

        return javaStreamingContextFactory.create();
    }

    private JavaStreamingContext getOrCreateWithCheckpointing() {
        String checkpointPath = optionalCheckpointPath.get();
        return JavaStreamingContext.getOrCreate(
            checkpointPath,
            (org.apache.spark.streaming.api.java.JavaStreamingContextFactory) () -> {
                JavaStreamingContext javaStreamingContext = javaStreamingContextFactory.create();
                javaStreamingContext.checkpoint(checkpointPath);
                return javaStreamingContext;
            }
        );
    }
}
