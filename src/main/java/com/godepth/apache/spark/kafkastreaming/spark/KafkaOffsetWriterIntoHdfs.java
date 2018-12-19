package com.godepth.apache.spark.kafkastreaming.spark;

import com.godepth.apache.spark.kafkastreaming.ThreadSleeper;
import com.godepth.apache.spark.kafkastreaming.hdfs.HdfsUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

public class KafkaOffsetWriterIntoHdfs implements KafkaOffsetWriter {

    private static final Logger LOGGER = Logger.getLogger(KafkaOffsetWriterIntoHdfs.class);

    private final HdfsUtils hdfsUtils;
    private final int maxAttempts;
    private final String offsetFilePrefix;
    private final int sleepBetweenRetriesInMs;
    private final ThreadSleeper threadSleeper;

    public KafkaOffsetWriterIntoHdfs(
        HdfsUtils hdfsUtils,
        int maxAttempts,
        String offsetFilePrefix,
        int sleepBetweenRetriesInMs,
        ThreadSleeper threadSleeper
    ) {
        this.hdfsUtils = hdfsUtils;
        this.maxAttempts = maxAttempts;
        this.offsetFilePrefix = offsetFilePrefix;
        this.sleepBetweenRetriesInMs = sleepBetweenRetriesInMs;
        this.threadSleeper = threadSleeper;
    }

    public void writeOffset(
        KafkaOffsetRange kafkaOffsetRange
    ) {
        Thread shutdownHook =
            new Thread(() -> writeOffsetHandler(kafkaOffsetRange));

        try {

            Runtime
                .getRuntime()
                .addShutdownHook(shutdownHook);

            writeOffsetHandler(kafkaOffsetRange);

        } finally {

            Runtime
                .getRuntime()
                .removeShutdownHook(shutdownHook);
        }
    }

    private void writeOffsetHandler(
        KafkaOffsetRange kafkaOffsetRange
    ) {
        int partition = kafkaOffsetRange.getPartition();
        Long offset = kafkaOffsetRange.getUntilOffset();

        String offsetFile = offsetFilePrefix + "." + partition;

        int count = 0;

        while (true) {

            try {

                LOGGER.info(
                    "Writing kafka partition " + partition +
                    " offset " + offset +
                    " into: " + offsetFile
                );

                hdfsUtils.writeText(
                    offsetFile,
                    offset.toString(),
                    true
                );

                return;

            } catch (IOException e) {

                if (++count == maxAttempts) {

                    throw new RuntimeException(
                        "Unable to write kafka partition " + partition +
                        " offset " + offset +
                        " into: " + offsetFile,
                        e
                    );
                }

                LOGGER.error(
                    "Unable to write kafka partition " + partition +
                    " offset " + offset +
                    " into: " + offsetFile +
                    " -- retrying in " + sleepBetweenRetriesInMs + "ms"
                );

                threadSleeper
                    .sleep(sleepBetweenRetriesInMs);
            }
        }
    }
}
