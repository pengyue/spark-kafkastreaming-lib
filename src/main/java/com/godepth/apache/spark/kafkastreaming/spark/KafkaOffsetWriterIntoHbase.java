package com.godepth.apache.spark.kafkastreaming.spark;

import com.godepth.apache.spark.kafkastreaming.ThreadSleeper;
import com.godepth.apache.spark.kafkastreaming.hbase.ConnectionFactory;
import com.godepth.apache.spark.kafkastreaming.hbase.SingleKeyTableMetadata;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

public class KafkaOffsetWriterIntoHbase implements KafkaOffsetWriter {

    private static final Logger LOGGER = Logger.getLogger(KafkaOffsetWriterIntoHbase.class);

    private final ConnectionFactory connectionFactory;
    private final SingleKeyTableMetadata tableMetadata;
    private final String topicGroupName;
    private final int maxAttempts;
    private final int sleepBetweenRetriesInMs;
    private final ThreadSleeper threadSleeper;

    private static transient Connection connection;

    public KafkaOffsetWriterIntoHbase(
        ConnectionFactory connectionFactory,
        SingleKeyTableMetadata tableMetadata,
        String topicGroupName,
        int maxAttempts,
        int sleepBetweenRetriesInMs,
        ThreadSleeper threadSleeper
    ) {
        this.connectionFactory = connectionFactory;
        this.tableMetadata = tableMetadata;
        this.topicGroupName = topicGroupName;
        this.maxAttempts = maxAttempts;
        this.sleepBetweenRetriesInMs = sleepBetweenRetriesInMs;
        this.threadSleeper = threadSleeper;
    }

    public void writeOffset(
        KafkaOffsetRange kafkaOffsetRange
    ) {
        tryConnect();

        int partition = kafkaOffsetRange.getPartition();
        long offset = kafkaOffsetRange.getUntilOffset();

        int count = 0;

        while (true) {

            try {

                LOGGER.info(
                    "Writing kafka partition " + partition +
                    " offset " + offset +
                    " into HBASE: " + topicGroupName
                );

                try (Table table =
                         connection
                             .getTable(tableMetadata.getTableName())) {

                    byte[] key = Bytes.toBytes(topicGroupName);
                    byte[] family = Bytes.toBytes(tableMetadata.getFamily());
                    byte[] qualifier = Bytes.toBytes(Integer.toString(partition));

                    Put put = new Put(key);

                    put.addColumn(family, qualifier, Bytes.toBytes(Long.toString(offset)));

                    table.put(put);

                    return;
                }

            } catch (IOException e) {

                if (++count == maxAttempts) {

                    throw new RuntimeException(
                        "Unable to write kafka partition " + partition +
                        " offset " + offset +
                        " into HBASE: " + topicGroupName,
                        e
                    );
                }

                LOGGER.error(
                    "Unable to write kafka partition " + partition +
                    " offset " + offset +
                    " into HBASE: " + topicGroupName +
                    " -- retrying in " + sleepBetweenRetriesInMs + "ms"
                );

                threadSleeper
                    .sleep(sleepBetweenRetriesInMs);
            }
        }
    }

    private void tryConnect() {

        if (connection != null) {
            return;
        }

        synchronized (this) {

            if (connection != null) {
                return;
            }

            connection =
                connectionFactory.create();
        }
    }
}
