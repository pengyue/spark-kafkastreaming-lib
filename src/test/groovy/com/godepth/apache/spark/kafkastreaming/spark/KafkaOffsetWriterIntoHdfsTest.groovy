package com.godepth.apache.spark.kafkastreaming.spark

import com.godepth.apache.spark.kafkastreaming.ThreadSleeper
import com.godepth.apache.spark.kafkastreaming.hdfs.HdfsUtils
import spock.lang.Specification

class KafkaOffsetWriterIntoHdfsTest extends Specification {

    def hdfsUtils = Mock(HdfsUtils)
    def maxAttempts = 2
    def offsetFilePrefix = "whatever"
    def sleepBetweenRetriesInMs = 10
    def threadSleeper = Mock(ThreadSleeper)
    def headOffset = 200
    def headOffsetAsString = "200"
    def partition = 1
    def offsetFileLocation = offsetFilePrefix + "." + partition
    def overwriteFile = true


    def KafkaOffsetWriterIntoHdfs

    def setup() {
        KafkaOffsetWriterIntoHdfs = new KafkaOffsetWriterIntoHdfs(
            hdfsUtils,
            maxAttempts,
            offsetFilePrefix,
            sleepBetweenRetriesInMs,
            threadSleeper
        )
    }

    def "Kafka Offset Writer writes offset for partition using offset range into HDFS"() {
        setup:

        def kafkaOffsetRange = Mock(KafkaOffsetRange) {
            getPartition() >> partition
            getUntilOffset() >> headOffset
        }

        when:

        KafkaOffsetWriterIntoHdfs.writeOffset(kafkaOffsetRange)

        then:

        1 * hdfsUtils.writeText(
            offsetFileLocation,
            headOffsetAsString,
            overwriteFile
        )
    }

    def "Kafka Offset Writer throws a runtime exception when max number of attempts has been reached"() {
        setup:

        def kafkaOffsetRange = Mock(KafkaOffsetRange) {
            getPartition() >> partition
            getUntilOffset() >> headOffset
        }

        when:

        KafkaOffsetWriterIntoHdfs.writeOffset(kafkaOffsetRange)

        then:

        2 * hdfsUtils.writeText(
            offsetFileLocation,
            headOffsetAsString,
            overwriteFile
        ) >> { throw new IOException("Error") }

        1 * threadSleeper.sleep(
            sleepBetweenRetriesInMs
        )

        thrown(RuntimeException)
    }

    def "Kafka Offset Writer retries after an IOException is thrown when writing to a file"() {
        setup:

        def kafkaOffsetRange = Mock(KafkaOffsetRange) {
            getPartition() >> partition
            getUntilOffset() >> headOffset
        }

        when:

        KafkaOffsetWriterIntoHdfs.writeOffset(kafkaOffsetRange)

        then:

        1 * hdfsUtils.writeText(
            offsetFileLocation,
            headOffsetAsString,
            overwriteFile
        ) >> { throw new IOException("Error") }

        1 * hdfsUtils.writeText(
            offsetFileLocation,
            headOffsetAsString,
            overwriteFile
        )

        1 * threadSleeper.sleep(
            sleepBetweenRetriesInMs
        )
    }
}
