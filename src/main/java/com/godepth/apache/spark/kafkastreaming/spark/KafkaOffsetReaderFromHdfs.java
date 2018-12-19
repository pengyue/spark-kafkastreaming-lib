package com.godepth.apache.spark.kafkastreaming.spark;

import com.godepth.apache.spark.kafkastreaming.hdfs.HdfsUtils;

import java.io.IOException;

public class KafkaOffsetReaderFromHdfs implements KafkaOffsetReader {

    private final HdfsUtils hdfsUtils;
    private final String offsetFilePrefix;

    public KafkaOffsetReaderFromHdfs(
        HdfsUtils hdfsUtils,
        String offsetFilePrefix
    ) {
        this.hdfsUtils = hdfsUtils;
        this.offsetFilePrefix = offsetFilePrefix;
    }

    public long readOffset(
        int partition
    ) {
        String offsetFile = offsetFilePrefix + "." + partition;

        try {

            String offset = hdfsUtils.readText(offsetFile);

            return Long.valueOf(offset);

        } catch (IOException | NumberFormatException e) {
            throw new RuntimeException("Unable to read kafka offset file from HDFS:" + offsetFile, e);
        }
    }
}
