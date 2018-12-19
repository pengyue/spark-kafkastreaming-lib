package com.godepth.apache.spark.kafkastreaming.spark

import spock.lang.Specification

class KafkaOffsetValidatorTest extends Specification {

    def kafkaOffsetReader = Mock(KafkaOffsetReader)

    def kafkaOffsetValidator

    def setup() {
        kafkaOffsetValidator = new KafkaOffsetValidator(
            kafkaOffsetReader
        )
    }

    def "Kafka Offset Validator can start from previous offset (no messages reconsumed)"() {
        setup:

        def partition = 1
        def previousOffset = 100
        def startOffset = 100

        def kafkaOffsetRange = Mock(KafkaOffsetRange) {
            getPartition() >> partition
            getFromOffset() >> startOffset
        }

        when:

        kafkaOffsetValidator.validateOffset(kafkaOffsetRange)

        then:

        1 * kafkaOffsetReader.readOffset(partition) >> previousOffset
    }

    def "Kafka Offset Validator can start before previous offset (some reconsumed messages)"() {
        setup:

        def partition = 1
        def previousOffset = 100
        def startOffset = 99

        def kafkaOffsetRange = Mock(KafkaOffsetRange) {
            getPartition() >> partition
            getFromOffset() >> startOffset
        }

        when:

        kafkaOffsetValidator.validateOffset(kafkaOffsetRange)

        then:

        1 * kafkaOffsetReader.readOffset(partition) >> previousOffset
    }

    def "Kafka Offset Validator will not start before previous offset (so that no messages are skipped)"() {
        setup:

        def partition = 1
        def previousOffset = 100
        def startOffset = 101

        def kafkaOffsetRange = Mock(KafkaOffsetRange) {
            getPartition() >> partition
            getFromOffset() >> startOffset
        }

        when:

        kafkaOffsetValidator.validateOffset(kafkaOffsetRange)

        then:

        1 * kafkaOffsetReader.readOffset(partition) >> previousOffset

        thrown(RuntimeException)
    }
}
