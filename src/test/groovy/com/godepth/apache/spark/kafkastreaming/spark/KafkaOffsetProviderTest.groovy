package com.godepth.apache.spark.kafkastreaming.spark

import spock.lang.Specification

class KafkaOffsetProviderTest extends Specification {

    def kafkaOffsetReader = Mock(KafkaOffsetReader)
    def topic = "stuff"
    def numPartitions = 2

    def kafkaOffsetProvider

    def setup() {
        kafkaOffsetProvider = new KafkaOffsetProvider(
            kafkaOffsetReader,
            topic,
            numPartitions
        )
    }

    def "Kafka Offset Resolver can resolve offsets for all partitions within a topic"() {
        setup:

        def offsetForPartition1 = 100
        def offsetForPartition2 = 200

        when:

        def offsets = kafkaOffsetProvider.getOffsets()

        def keyOne = offsets.keySet().toArray()[0]
        def valOne = offsets.get(keyOne)

        def keyTwo = offsets.keySet().toArray()[1]
        def valTwo = offsets.get(keyTwo)

        then:

        1 * kafkaOffsetReader.readOffset(0) >> offsetForPartition1
        1 * kafkaOffsetReader.readOffset(1) >> offsetForPartition2

        keyOne.topic == topic
        keyOne.partition == 0
        valOne == offsetForPartition1

        keyTwo.topic == topic
        keyTwo.partition == 1
        valTwo == offsetForPartition2
    }
}
