package com.godepth.apache.spark.kafkastreaming.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.Serializable;

public interface SingleKeyTableMetadata extends Serializable {

    TableName getTableName();

    String getFamily();

    String getQualifier();

    byte[][] getSplits();

    Compression.Algorithm getCompressionAlgorithm();

    int getMaxVersions();

    boolean hasTimeToLive();

    int getTimeToLive();
}
