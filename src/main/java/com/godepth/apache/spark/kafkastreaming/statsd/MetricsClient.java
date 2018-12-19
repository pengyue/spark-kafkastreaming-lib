package com.godepth.apache.spark.kafkastreaming.statsd;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsClient.class);
    private static final Runtime RUNTIME = Runtime.getRuntime();

    private final NonBlockingStatsDClient statsDClient;
    private final String serviceName;

    public MetricsClient(
        NonBlockingStatsDClient statsDClient,
        String serviceName
    ) {
        this.statsDClient = statsDClient;
        this.serviceName = serviceName;
    }

    public void incrementFailure(
        long delta
    ) {
        if (statsDClient != null) {
            try {
                statsDClient.count(serviceName + ".failure", delta);
            } catch (StatsDClientException e) {
                LOGGER.warn(e.getMessage());
            }
        }
    }

    public void incrementSuccess(
        long delta
    ) {
        if (statsDClient != null) {
            try {
                statsDClient.count(serviceName + ".success", delta);
            } catch (StatsDClientException e) {
                LOGGER.warn(e.getMessage());
            }
        }
    }

    public void recordBacklog(
        String topic,
        int partition,
        long size
    ) {
        if (statsDClient != null) {
            try {
                statsDClient.gauge(serviceName + "." + topic + "." + partition + ".backlog", size);
            } catch (StatsDClientException e) {
                LOGGER.warn(e.getMessage());
            }
        }
    }

    public void recordBatchTime(
        int batchSize,
        long timeInMs
    ) {
        if (statsDClient != null) {
            try {

                if (batchSize > 0) {

                    statsDClient.recordExecutionTime(serviceName + ".time.batch", timeInMs);
                    statsDClient.recordExecutionTime(serviceName + ".time.element", Math.round(timeInMs / batchSize));
                }

            } catch (StatsDClientException e) {
                LOGGER.warn(e.getMessage());
            }
        }
    }

    public void recordMemoryUsage(
        int partition
    ) {
        if (statsDClient != null) {
            try {

                long maxMemory = RUNTIME.maxMemory();
                long allocatedMemory = RUNTIME.totalMemory();
                long freeMemory = RUNTIME.freeMemory();

                long absoluteMemoryUsed = (allocatedMemory - freeMemory);
                double percentageMemoryUsed = ((absoluteMemoryUsed * 1.0) / allocatedMemory) * 100;

                statsDClient.gauge(serviceName + "." + partition + ".memory.max", (maxMemory / 1024));
                statsDClient.gauge(serviceName + "." + partition + ".memory.allocated", (allocatedMemory / 1024));
                statsDClient.gauge(serviceName + "." + partition + ".memory.free", (freeMemory / 1024));
                statsDClient.gauge(serviceName + "." + partition + ".memory.used", (absoluteMemoryUsed / 1024));
                statsDClient.gauge(serviceName + "." + partition + ".memory.usedPercentage", percentageMemoryUsed);

            } catch (StatsDClientException e) {
                LOGGER.warn(e.getMessage());
            }
        }
    }
}
