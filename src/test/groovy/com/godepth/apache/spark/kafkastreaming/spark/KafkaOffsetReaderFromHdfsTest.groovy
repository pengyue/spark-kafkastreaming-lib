package com.godepth.apache.spark.kafkastreaming.spark

import com.godepth.apache.spark.kafkastreaming.hdfs.HdfsUtils
import spock.lang.Specification

class KafkaOffsetReaderFromHdfsTest extends Specification {

    def hdfsUtils = Mock(HdfsUtils)
    def offsetFilePrefix = "whatever"

    def kafkaOffsetReaderFromHdfs

    def setup() {
        kafkaOffsetReaderFromHdfs = new KafkaOffsetReaderFromHdfs(
            hdfsUtils,
            offsetFilePrefix
        )
    }

    def "Kafka Offset Reader reads offset for partition from HDFS"() {
        setup:

        def partition = 1
        def storedOffset = "123"

        when:

        def offset = kafkaOffsetReaderFromHdfs.readOffset(partition)

        then:

        1 * hdfsUtils.readText(offsetFilePrefix + "." + partition) >> storedOffset

        offset == Long.valueOf(storedOffset)
    }
}
