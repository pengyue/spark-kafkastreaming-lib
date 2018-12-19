package com.godepth.apache.spark.kafkastreaming.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShutdownMonitor {

    private static final Logger LOGGER = Logger.getLogger(ShutdownMonitor.class);

    private final String stopRequestFile;
    private final int timeout;

    public ShutdownMonitor(
        String stopRequestFile,
        int timeout
    ) {
        this.stopRequestFile = stopRequestFile;
        this.timeout = timeout;
    }

    public void stopOnShutdown(
        JavaStreamingContext javaStreamingContext
    ) throws IOException {

        AtomicBoolean isContextStopping = new AtomicBoolean(false);
        AtomicBoolean hasPrintedIOExceptionStacktrace = new AtomicBoolean(false);

        Configuration hadoopConfiguration = new Configuration();
        FileSystem hadoopFileSystem = FileSystem.get(hadoopConfiguration);
        Path stopRequestFilePath = new Path(stopRequestFile);

        new Thread(() -> {

            while (true) {

                try {

                    if (hadoopFileSystem.exists(stopRequestFilePath)) {

                        new Thread(() -> {

                            try {

                                Thread.sleep(timeout);

                                deleteStopRequestFile(hadoopFileSystem, stopRequestFilePath);

                                System.exit(0);

                            } catch (InterruptedException e) {
                                // noop

                            } catch (IOException e) {
                                if (hasPrintedIOExceptionStacktrace.compareAndSet(false, true)) {
                                    e.printStackTrace();
                                }
                            }

                        }).start();

                        if (isContextStopping.compareAndSet(false, true)) {
                            LOGGER.info("@@ Stopping application ...");
                            Thread.sleep(2000);
                            deleteStopRequestFile(hadoopFileSystem, stopRequestFilePath);
                            LOGGER.info("@@ Stopped application");
                        }

                        System.exit(0);
                    }

                    Thread.sleep(1000);

                } catch (InterruptedException e) {
                    // noop

                } catch (IOException e) {
                    if (hasPrintedIOExceptionStacktrace.compareAndSet(false, true)) {
                        e.printStackTrace();
                    }
                }
            }

        }).start();
    }

    private void deleteStopRequestFile(
        FileSystem hadoopFileSystem,
        Path stopRequestFilePath
    ) throws IOException {

        if (hadoopFileSystem.exists(stopRequestFilePath)) {
            LOGGER.info("@@ Deleting stop request file ...");
            if (hadoopFileSystem.delete(stopRequestFilePath, false)) {
                LOGGER.info("@@ Deleted stop request file");
            } else {
                LOGGER.error("@@ Failed to delete stop request file");
                throw new IOException("Cannot delete stop request file");
            }
        }
    }
}
