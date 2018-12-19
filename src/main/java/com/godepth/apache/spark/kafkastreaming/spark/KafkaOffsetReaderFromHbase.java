package com.godepth.apache.spark.kafkastreaming.spark;

import com.godepth.apache.spark.kafkastreaming.hbase.ConnectionFactory;
import com.godepth.apache.spark.kafkastreaming.hbase.SingleKeyTableMetadata;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class KafkaOffsetReaderFromHbase implements KafkaOffsetReader {

    private final ConnectionFactory connectionFactory;
    private final SingleKeyTableMetadata tableMetadata;
    private final String topicGroupName;

    private static transient Connection connection;

    public KafkaOffsetReaderFromHbase(
        ConnectionFactory connectionFactory,
        SingleKeyTableMetadata tableMetadata,
        String topicGroupName
    ) {
        this.connectionFactory = connectionFactory;
        this.tableMetadata = tableMetadata;
        this.topicGroupName = topicGroupName;
    }

    public long readOffset(
        int partition
    ) {
        tryConnect();

        try (Table table =
                 connection
                     .getTable(tableMetadata.getTableName())) {

            byte[] key = Bytes.toBytes(topicGroupName);
            byte[] family = Bytes.toBytes(tableMetadata.getFamily());
            byte[] qualifier = Bytes.toBytes(Integer.toString(partition));

            Result result =
                table.get(
                    new Get(key)
                );

            if (result.isEmpty()) {

                throw new RuntimeException(
                    "Unable to read kafka offset for partition " +
                    partition +
                    " from HBASE: " +
                    topicGroupName
                );
            }

            return Long.valueOf(
                Bytes.toString(
                    result.getValue(family, qualifier)
                )
            );

        } catch (IOException | IllegalArgumentException e) {

            throw new RuntimeException(
                "Unable to read kafka offset for partition " +
                partition +
                " from HBASE: " +
                topicGroupName,
                e
            );
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
