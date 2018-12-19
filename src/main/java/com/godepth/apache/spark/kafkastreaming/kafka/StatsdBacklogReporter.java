package com.godepth.apache.spark.kafkastreaming.kafka;

import com.godepth.apache.spark.kafkastreaming.statsd.MetricsClient;

public class StatsdBacklogReporter implements BacklogReporter {

    private final MetricsClient metricsClient;

    public StatsdBacklogReporter(
        MetricsClient metricsClient
    ) {
        this.metricsClient = metricsClient;
    }

    public void report(
        String topic,
        int partition,
        long backlog
    ) {
        metricsClient
            .recordBacklog(
                topic,
                partition,
                backlog
            );
    }
}
