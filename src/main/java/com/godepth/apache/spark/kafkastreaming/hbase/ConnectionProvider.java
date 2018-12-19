package com.godepth.apache.spark.kafkastreaming.hbase;

import org.apache.hadoop.hbase.client.Connection;

import java.io.Serializable;

public interface ConnectionProvider extends Serializable {

    Connection provide();
}
