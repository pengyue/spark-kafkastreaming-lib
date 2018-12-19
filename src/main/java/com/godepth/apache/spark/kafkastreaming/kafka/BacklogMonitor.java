package com.godepth.apache.spark.kafkastreaming.kafka;

import com.godepth.apache.spark.kafkastreaming.ThreadSleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class BacklogMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BacklogMonitor.class);

    private final BacklogCalculator backlogCalculator;
    private final BacklogReporter backlogReporter;
    private final ThreadSleeper threadSleeper;
    private final String topic;
    private final int numberOfPartitions;

    private final AtomicBoolean isMonitorRunning = new AtomicBoolean(false);

    public BacklogMonitor(
        BacklogCalculator backlogCalculator,
        BacklogReporter backlogReporter,
        ThreadSleeper threadSleeper,
        String topic,
        int numberOfPartitions
    ) {
        this.backlogCalculator = backlogCalculator;
        this.backlogReporter = backlogReporter;
        this.threadSleeper = threadSleeper;
        this.topic = topic;
        this.numberOfPartitions = numberOfPartitions;
    }

    public void tryStartBackgroundMonitor() {

        if (isMonitorRunning.getAndSet(true)) {
            return;
        }

        new Thread(() -> {

            LOGGER.info("@@ Backlog Monitor is running ...");

            while (true) {

                for (int partition = 0; partition < numberOfPartitions; partition++) {

                    Optional<Long> backlog =
                        backlogCalculator
                            .calculate(topic, partition);

                    if (backlog.isPresent()) {

                        LOGGER.info(
                            "@@ Backlog for " + topic + ":" + partition +
                            " is " + backlog.get()
                        );

                        backlogReporter
                            .report(
                                topic,
                                partition,
                                backlog.get()
                            );

                    } else {

                        LOGGER.info(
                            "@@ Backlog for " + topic + ":" + partition +
                            " is <unknown>"
                        );
                    }
                }

                threadSleeper
                    .sleep(1000);
            }

        }).start();
    }
}
