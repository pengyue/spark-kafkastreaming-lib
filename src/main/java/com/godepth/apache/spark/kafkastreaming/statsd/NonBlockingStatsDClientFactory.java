package com.godepth.apache.spark.kafkastreaming.statsd;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class NonBlockingStatsDClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingStatsDClientFactory.class);

    private NonBlockingStatsDClientFactory() {
    }

    public static NonBlockingStatsDClient create(
        String appName,
        String host,
        int port
    ) {
        try {

            String qualifiedAppName = appName + "." + getHostName();

            return new NonBlockingStatsDClient(
                qualifiedAppName,
                host,
                port
            );

        } catch (StatsDClientException e) {
            LOGGER.warn(e.getMessage());
            return null;
        }
    }

    private static String getHostName() {
        try {
            final InetAddress localHost = InetAddress.getLocalHost();
            return localHost.getHostName().replace('.', '_');
        } catch (UnknownHostException e) {
            return "";
        }
    }
}
