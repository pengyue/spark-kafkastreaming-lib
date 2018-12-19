package integration

import com.godepth.apache.spark.kafkastreaming.spark.JavaStreamingContextFactory
import com.godepth.apache.spark.kafkastreaming.spark.JavaStreamingContextProvider
import integration.helper.ArtifactName
import integration.helper.CollectingStringSparkStreamConsumer
import integration.helper.EmbeddedKafka
import integration.helper.EmbeddedKafkaProducer
import integration.helper.FilesystemHelper
import integration.helper.KafkaStreamingContextFactoryUsingHdfsFactory
import spock.lang.Specification

class ConsumerUsingHdfsOffsetsIntegrationTest extends Specification {

    def TOPIC = this.class.simpleName
    def NUM_PARTITIONS = 2

    EmbeddedKafka embeddedKafka
    EmbeddedKafkaProducer embeddedKafkaProducer

    JavaStreamingContextProvider javaStreamingContextProvider
    CollectingStringSparkStreamConsumer sparkStreamConsumer

    String kafkaOffsetFilePartition0
    String kafkaOffsetFilePartition1

    String tmpDirectory

    def setup() {

        FilesystemHelper.createTmp(ArtifactName.getArtifactName())
        tmpDirectory = FilesystemHelper.getTmpDirectory(ArtifactName.getArtifactName())

        embeddedKafka = new EmbeddedKafka()
        embeddedKafka.start()

        embeddedKafkaProducer = new EmbeddedKafkaProducer(embeddedKafka)

        FilesystemHelper.createTmp(tmpDirectory + "/checkpoint")
        def sparkCheckpointDirectory = FilesystemHelper.getTmpDirectory(tmpDirectory + "/checkpoint")

        FilesystemHelper.createTmp(tmpDirectory + "/work")
        def sparkWorkDirectory = FilesystemHelper.getTmpDirectory(tmpDirectory + "/work")

        def kafkaOffsetFilePrefix = tmpDirectory + "/kafka.offset"

        kafkaOffsetFilePartition0 = kafkaOffsetFilePrefix + ".0"
        kafkaOffsetFilePartition1 = kafkaOffsetFilePrefix + ".1"

        new File(kafkaOffsetFilePartition0).write("0")
        new File(kafkaOffsetFilePartition1).write("0")

        def batchDurationInSeconds = 1

        def kafkaProperties = [
            "acks"                     : "1",
            "batch.size"               : "1",
            "bootstrap.servers"        : embeddedKafka.kafkaConnectString,
            "metadata.fetch.timeout.ms": "100",
        ]

        def sparkProperties = [
            "spark.app.name"                           : this.class.simpleName,
            "spark.local.dir"                          : sparkWorkDirectory,
            "spark.master"                             : "local[1]",
            "spark.serializer"                         : "org.apache.spark.serializer.KryoSerializer",
            "spark.ui.enabled"                         : "false",
            "spark.ui.showConsoleProgress"             : "false",
            "spark.streaming.kafka.maxRatePerPartition": "2",
        ]

        sparkStreamConsumer = new CollectingStringSparkStreamConsumer()

        JavaStreamingContextFactory kafkaStreamingContextFactory =
            KafkaStreamingContextFactoryUsingHdfsFactory.create(
                batchDurationInSeconds,
                kafkaOffsetFilePrefix,
                TOPIC,
                NUM_PARTITIONS,
                kafkaProperties,
                sparkProperties,
                sparkStreamConsumer,
            )

        javaStreamingContextProvider = new JavaStreamingContextProvider(
            kafkaStreamingContextFactory,
            sparkCheckpointDirectory
        )
    }

    def "Spark Streaming consumes messages from Kafka"() {
        setup:

        embeddedKafkaProducer.createTopic(TOPIC, NUM_PARTITIONS)

        embeddedKafkaProducer.sendMessage(TOPIC, "1", "message-1")
        embeddedKafkaProducer.sendMessage(TOPIC, "2", "message-2")
        embeddedKafkaProducer.sendMessage(TOPIC, "3", "message-3")

        when:

        def javaStreamingContext = javaStreamingContextProvider.getOrCreate()

        startStreamProcessing(javaStreamingContext)

        int sleepAttempts = 320 // max 32 seconds
        while (sleepAttempts-- > 0) {

            if (sparkStreamConsumer.COLLECTED_MESSAGES.containsKey(TOPIC)
                && sparkStreamConsumer.COLLECTED_MESSAGES.get(TOPIC).size() == NUM_PARTITIONS) {
                break
            }

            Thread.sleep(100)
        }

        stopStreamProcessing(javaStreamingContext)

        then:

        sparkStreamConsumer.COLLECTED_MESSAGES.containsKey(TOPIC)

        def collectedStreams = sparkStreamConsumer.COLLECTED_MESSAGES.get(TOPIC)

        collectedStreams.size() == NUM_PARTITIONS

        def partition0
        def partition1

        if (collectedStreams.get(0).size() == 1) {
            partition0 = collectedStreams.get(0)
            partition1 = collectedStreams.get(1)
        } else {
            partition0 = collectedStreams.get(1)
            partition1 = collectedStreams.get(0)
        }

        partition0.size() == 1
        partition0.toList().get(0) == "message-2"

        partition1.size() == 2
        partition1.toList().get(0) == "message-1"
        partition1.toList().get(1) == "message-3"

        when:

        def storedKafkaOffsetPartition0 =
            new File(kafkaOffsetFilePartition0)
                .getText()
                .toLong()

        def storedKafkaOffsetPartition1 =
            new File(kafkaOffsetFilePartition1)
                .getText()
                .toLong()

        then:
        storedKafkaOffsetPartition0 == 1
        storedKafkaOffsetPartition1 == 2
    }

    def startStreamProcessing(javaStreamingContext) {

        def thread = new Thread() {
            public void run() {
                javaStreamingContext.start()
            }
        }

        thread.start()
    }

    def stopStreamProcessing(javaStreamingContext) {
        try {
            javaStreamingContext.stop(true, true)
            Thread.sleep(1000)
        } catch (Exception e) {
        }
    }

    def cleanup() {

        embeddedKafka.stop()

        def tmpDirectory = System.getProperty("java.io.tmpdir") + "/";

        // delete kafka temp directories
        new File(tmpDirectory).eachDirMatch(~/[0-9]{13}-0.*/) { dir ->
            dir.deleteDir()
        }

        FilesystemHelper.deleteTmp(ArtifactName.getArtifactName())
    }
}
