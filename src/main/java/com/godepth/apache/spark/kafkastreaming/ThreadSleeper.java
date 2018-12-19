package com.godepth.apache.spark.kafkastreaming;

import java.io.Serializable;

public class ThreadSleeper implements Serializable {

    public void sleep(
        long sleepDelay
    ) {
        try {
            Thread.sleep(sleepDelay);
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread sleep interrupted", e);
        }
    }
}
