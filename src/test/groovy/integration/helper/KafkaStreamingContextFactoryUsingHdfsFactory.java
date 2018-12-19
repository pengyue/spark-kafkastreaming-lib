package integration.helper;

import com.godepth.apache.spark.kafkastreaming.ThreadSleeper;
import com.godepth.apache.spark.kafkastreaming.hdfs.HdfsUtils;
import com.godepth.apache.spark.kafkastreaming.spark.JavaSparkContextFactory;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaInputStreamConsumer;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaInputStreamFactory;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetProvider;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetReader;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetReaderFromHdfs;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetValidator;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetWriter;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetWriterIntoHdfs;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaStreamingContextFactory;
import com.godepth.apache.spark.kafkastreaming.spark.SparkStreamConsumer;
import com.godepth.apache.spark.kafkastreaming.spark.SparkStreamConsumerBytesToStringTranscoder;

import java.util.Map;

public class KafkaStreamingContextFactoryUsingHdfsFactory {

    public static KafkaStreamingContextFactory<String> create(
        int batchDurationInSeconds,
        String kafkaOffsetFilePrefix,
        String kafkaTopic,
        int kafkaNumberOfPartitions,
        Map<String, String> kafkaProperties,
        Map<String, String> sparkProperties,
        SparkStreamConsumer<String> sparkStreamConsumer
    ) {
        HdfsUtils hdfsUtils = new HdfsUtils();
        JavaSparkContextFactory javaSparkContextFactory = new JavaSparkContextFactory(sparkProperties);
        KafkaInputStreamConsumer<String> kafkaInputStreamConsumer = new KafkaInputStreamConsumer<>();
        KafkaInputStreamFactory kafkaInputStreamFactory = new KafkaInputStreamFactory(kafkaProperties);
        SparkStreamConsumerBytesToStringTranscoder sparkStreamConsumerTranscoder = new SparkStreamConsumerBytesToStringTranscoder();
        ThreadSleeper threadSleeper = new ThreadSleeper();

        KafkaOffsetReader kafkaOffsetReader = new KafkaOffsetReaderFromHdfs(
            hdfsUtils,
            kafkaOffsetFilePrefix
        );

        KafkaOffsetWriter kafkaOffsetWriter = new KafkaOffsetWriterIntoHdfs(
            hdfsUtils,
            5,
            kafkaOffsetFilePrefix,
            100,
            threadSleeper
        );

        KafkaOffsetProvider kafkaOffsetProvider = new KafkaOffsetProvider(kafkaOffsetReader, kafkaTopic, kafkaNumberOfPartitions);
        KafkaOffsetValidator kafkaOffsetValidator = new KafkaOffsetValidator(kafkaOffsetReader);

        return new KafkaStreamingContextFactory<>(
            batchDurationInSeconds,
            javaSparkContextFactory,
            kafkaInputStreamConsumer,
            kafkaInputStreamFactory,
            kafkaOffsetProvider,
            kafkaOffsetValidator,
            kafkaOffsetWriter,
            () -> sparkStreamConsumer,
            sparkStreamConsumerTranscoder
        );
    }
}
