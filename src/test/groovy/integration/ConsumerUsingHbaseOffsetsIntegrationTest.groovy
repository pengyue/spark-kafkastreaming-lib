package integration

import com.godepth.apache.spark.kafkastreaming.hbase.SingleKeyTableMetadata
import com.godepth.apache.spark.kafkastreaming.spark.JavaStreamingContextFactory
import com.godepth.apache.spark.kafkastreaming.spark.JavaStreamingContextProvider
import integration.helper.ArtifactName
import integration.helper.CollectingStringSparkStreamConsumer
import integration.helper.EmbeddedKafka
import integration.helper.EmbeddedKafkaProducer
import integration.helper.FilesystemHelper
import integration.helper.KafkaStreamingContextFactoryUsingHbaseFactory
import integration.helper.TestHbaseTableMetadata
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.api.java.JavaStreamingContext
import spock.lang.Specification

class ConsumerUsingHbaseOffsetsIntegrationTest extends Specification {

    static HBaseTestingUtility embeddedHbase

    static SingleKeyTableMetadata hbaseTableMetadata
    static String tmpDirectory

    Table hbaseTable

    CollectingStringSparkStreamConsumer sparkStreamConsumer

    EmbeddedKafka embeddedKafka
    EmbeddedKafkaProducer embeddedKafkaProducer


    def setupSpec() {

        FilesystemHelper.createTmp(ArtifactName.getArtifactName())
        tmpDirectory = FilesystemHelper.getTmpDirectory(ArtifactName.getArtifactName())

        System.setProperty("test.build.data.basedirectory", tmpDirectory + "/hbase")

        // initialise HBaseTestingUtility once and use if for all the tests
        // this is because it doesn't seem to shutdown, destroy and then restart cleanly
        embeddedHbase = new HBaseTestingUtility()
        embeddedHbase.getConfiguration().setLong("hbase.client.pause", 1)
        embeddedHbase.getConfiguration().setLong("hbase.master.event.waiting.time", 1)
        embeddedHbase.startMiniCluster()

        hbaseTableMetadata =
            new TestHbaseTableMetadata()

        setupTestTable(
            embeddedHbase.getConnection(),
            hbaseTableMetadata
        )

    }

    def setup() {

        embeddedKafka = new EmbeddedKafka()
        embeddedKafka.start()

        embeddedKafkaProducer = new EmbeddedKafkaProducer(embeddedKafka)

        hbaseTable =
            embeddedHbase.getConnection()
                .getTable(hbaseTableMetadata.getTableName())
    }

    def "Spark Streaming consumes messages from Kafka"() {
        setup:

        def topicName = "test_hbase_offsets"
        def numPartitions = 2
        def kafkaOffsetTopicGroupName = "test_hbase_offsets"

        def javaStreamingContextProvider = new JavaStreamingContextProvider(
            createKafkaStreamingContextFactory(topicName, numPartitions, kafkaOffsetTopicGroupName)
        )

        embeddedKafkaProducer.createTopic(topicName, numPartitions)
        resetKafkaOffsetsInHbase(hbaseTable, numPartitions, kafkaOffsetTopicGroupName)

        embeddedKafkaProducer.sendMessage(topicName, "1", "message-1")
        embeddedKafkaProducer.sendMessage(topicName, "2", "message-2")
        embeddedKafkaProducer.sendMessage(topicName, "3", "message-3")


        when:

        def javaStreamingContext = javaStreamingContextProvider.getOrCreate()

        startStreamProcessing(javaStreamingContext)

        int sleepAttempts = 320 // max 32 seconds
        while (sleepAttempts-- > 0) {

            if (sparkStreamConsumer.COLLECTED_MESSAGES.containsKey(topicName)
                && sparkStreamConsumer.COLLECTED_MESSAGES.get(topicName).size() == numPartitions) {
                break
            }

            Thread.sleep(100)
        }

        stopStreamProcessing(javaStreamingContext)

        then:

        sparkStreamConsumer.COLLECTED_MESSAGES.containsKey(topicName)

        def collectedStreams = sparkStreamConsumer.COLLECTED_MESSAGES.get(topicName)

        collectedStreams.size() == numPartitions

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

        fetchPartitionOffset(hbaseTable, kafkaOffsetTopicGroupName, 0) == 1
        fetchPartitionOffset(hbaseTable, kafkaOffsetTopicGroupName, 1) == 2
    }

    def "Spark Streaming consumes messages from Kafka with checkpointing"() {
        setup:

        def topicName = "test_hbase_offsets_with_checkpointing"
        def numPartitions = 2
        def kafkaOffsetTopicGroupName = "test_hbase_offsets_with_checkpointing"

        FilesystemHelper.createTmp(tmpDirectory + "/checkpoint")
        def sparkCheckpointDirectory = FilesystemHelper.getTmpDirectory(tmpDirectory + "/checkpoint")

        def javaStreamingContextProviderWithCheckpointing = new JavaStreamingContextProvider(
            createKafkaStreamingContextFactory(topicName, numPartitions, kafkaOffsetTopicGroupName),
            sparkCheckpointDirectory
        )

        embeddedKafkaProducer.createTopic(topicName, numPartitions)
        resetKafkaOffsetsInHbase(hbaseTable, numPartitions, kafkaOffsetTopicGroupName)

        embeddedKafkaProducer.sendMessage(topicName, "1", "message-1")
        embeddedKafkaProducer.sendMessage(topicName, "2", "message-2")
        embeddedKafkaProducer.sendMessage(topicName, "3", "message-3")


        when:

        def javaStreamingContext = javaStreamingContextProviderWithCheckpointing.getOrCreate()

        startStreamProcessing(javaStreamingContext)

        int sleepAttempts = 320 // max 32 seconds
        while (sleepAttempts-- > 0) {

            if (sparkStreamConsumer.COLLECTED_MESSAGES.containsKey(topicName)
                && sparkStreamConsumer.COLLECTED_MESSAGES.get(topicName).size() == numPartitions) {
                break
            }

            Thread.sleep(100)
        }

        stopStreamProcessing(javaStreamingContext)

        then:

        sparkStreamConsumer.COLLECTED_MESSAGES.containsKey(topicName)

        def collectedStreams = sparkStreamConsumer.COLLECTED_MESSAGES.get(topicName)

        collectedStreams.size() == numPartitions

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

        fetchPartitionOffset(hbaseTable, kafkaOffsetTopicGroupName, 0) == 1
        fetchPartitionOffset(hbaseTable, kafkaOffsetTopicGroupName, 1) == 2
    }

    JavaStreamingContextFactory createKafkaStreamingContextFactory(
        String topicName,
        Integer numPartitions,
        String kafkaOffsetTopicGroupName
    ) {

        FilesystemHelper.createTmp(tmpDirectory + "/work")
        def sparkWorkDirectory = FilesystemHelper.getTmpDirectory(tmpDirectory + "/work")

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

        return  KafkaStreamingContextFactoryUsingHbaseFactory.create(
            batchDurationInSeconds,
            embeddedHbase.getConfiguration(),
            hbaseTableMetadata,
            kafkaOffsetTopicGroupName,
            topicName,
            numPartitions,
            kafkaProperties,
            sparkProperties,
            sparkStreamConsumer,
        )
    }

    def startStreamProcessing(JavaStreamingContext javaStreamingContext) {

        def thread = new Thread() {
            public void run() {
                javaStreamingContext.start()
            }
        }

        thread.start()
    }

    def stopStreamProcessing(JavaStreamingContext javaStreamingContext) {
        try {
            javaStreamingContext.stop(true, true)
            Thread.sleep(1000)
        } catch (Exception e) {
        }
    }

    def setupTestTable(
        Connection hbaseConnection,
        SingleKeyTableMetadata hbaseTableMetadata
    ) {
        Admin admin = hbaseConnection.getAdmin()

        HColumnDescriptor columnDescriptor =
            new HColumnDescriptor(hbaseTableMetadata.getFamily())

        columnDescriptor
            .setCompressionType(hbaseTableMetadata.getCompressionAlgorithm())
            .setMaxVersions(hbaseTableMetadata.getMaxVersions())

        if (hbaseTableMetadata.hasTimeToLive()) {
            columnDescriptor.setTimeToLive(hbaseTableMetadata.getTimeToLive())
        }

        HTableDescriptor tableDescriptor =
            new HTableDescriptor(hbaseTableMetadata.getTableName())

        tableDescriptor
            .addFamily(columnDescriptor)

        admin.createTable(
            tableDescriptor,
            hbaseTableMetadata.getSplits()
        )

    }

    def resetKafkaOffsetsInHbase(
        Table table,
        Integer numPartitions,
        String kafkaOffsetTopicGroupName
    ) {

        byte[] key = Bytes.toBytes(kafkaOffsetTopicGroupName)
        byte[] family = Bytes.toBytes(hbaseTableMetadata.getFamily())

        for (int partition = 0; partition < numPartitions; partition++) {

            byte[] qualifier = Bytes.toBytes(Integer.toString(partition))

            Put put = new Put(key)

            put.addColumn(family, qualifier, Bytes.toBytes(Long.toString(0L)))

            table.put(put)
        }
    }

    def fetchPartitionOffset(
        Table table,
        String kafkaOffsetTopicGroupName,
        int partition
    ) {
        byte[] key = Bytes.toBytes(kafkaOffsetTopicGroupName)
        byte[] family = Bytes.toBytes(hbaseTableMetadata.getFamily())
        byte[] qualifier = Bytes.toBytes(Integer.toString(partition))

        Result result =
            table.get(
                new Get(key)
            )

        return Long.valueOf(
            Bytes.toString(
                result.getValue(family, qualifier)
            )
        )
    }

    def cleanup() {

        embeddedKafka.stop()
    }

    def cleanupSpec() {

        embeddedHbase.shutdownMiniHBaseCluster()

        def tmpDirectory = System.getProperty("java.io.tmpdir") + "/";

        // delete kafka temp directories
        new File(tmpDirectory).eachDirMatch(~/[0-9]{13}-0.*/) { dir ->
            dir.deleteDir()
        }

        FilesystemHelper.deleteTmp(ArtifactName.getArtifactName())
    }
}
