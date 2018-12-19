package com.godepth.apache.spark.kafkastreaming.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class ConnectionFactory implements Serializable {

    private static final Logger LOGGER = Logger.getLogger(ConnectionFactory.class);

    private final Map<String, String> hbaseProperties;

    public ConnectionFactory(
        Map<String, String> hbaseProperties
    ) {
        this.hbaseProperties = hbaseProperties;
    }

    public Connection create() {

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseProperties
            .entrySet()
            .forEach(
                hbaseProperty -> {
                    if (hbaseProperty.getValue() != null) {

                        hbaseConfiguration.set(
                            hbaseProperty.getKey(),
                            hbaseProperty.getValue()
                        );
                    }
                }
            );

        try {

            LOGGER.info("Connecting to hbase:\n" + hbaseConfiguration.toString());

            return org.apache.hadoop.hbase.client.ConnectionFactory
                .createConnection(hbaseConfiguration);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
