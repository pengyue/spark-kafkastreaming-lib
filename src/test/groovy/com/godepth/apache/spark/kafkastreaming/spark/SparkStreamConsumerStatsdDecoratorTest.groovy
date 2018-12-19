package com.godepth.apache.spark.kafkastreaming.spark


import com.godepth.apache.spark.kafkastreaming.statsd.MetricsClient
import spock.lang.Specification

class SparkStreamConsumerStatsdDecoratorTest extends Specification {

    def events = [1, 2]

    def topic = "type"
    def partition = 1

    def metricsClient = Mock(MetricsClient)

    def "Spark Stream Consumer success"() {
        setup:

        def sparkStreamConsumer = Mock(SparkStreamConsumer)
        def sparkStreamConsumerStatsdDecorator = new SparkStreamConsumerStatsdDecorator(
            sparkStreamConsumer,
            metricsClient,
        )

        when:

        sparkStreamConsumerStatsdDecorator.consume(topic, partition, events)

        then:

        1 * sparkStreamConsumer.consume(topic, partition, events)
        1 * metricsClient.incrementSuccess(events.size())
        1 * metricsClient.recordBatchTime(events.size(), _)
        1 * metricsClient.recordMemoryUsage(1)
    }

    def "Spark Stream Consumer failure"() {
        setup:

        def sparkStreamConsumer = Mock(SparkStreamConsumer)

        sparkStreamConsumer.consume(topic, partition, events) >> { throw new RuntimeException("Error") }

        def sparkStreamConsumerStatsdDecorator = new SparkStreamConsumerStatsdDecorator(
            sparkStreamConsumer,
            metricsClient,
        )

        when:

        sparkStreamConsumerStatsdDecorator.consume(topic, partition, events)

        then:

        0 * metricsClient.incrementSuccess(_)
        0 * metricsClient.recordBatchTime(_)
        0 * metricsClient.recordMemoryUsage(_)

        1 * metricsClient.incrementFailure(events.size())
        def e = thrown(RuntimeException)
        e.message == "Error"
    }
}
