package integration.helper;

import com.godepth.apache.spark.kafkastreaming.IterableToStream;
import com.godepth.apache.spark.kafkastreaming.ThreadSleeper;
import com.godepth.apache.spark.kafkastreaming.hbase.ConnectionFactory;
import com.godepth.apache.spark.kafkastreaming.hbase.SingleKeyTableMetadata;
import com.godepth.apache.spark.kafkastreaming.spark.JavaSparkContextFactory;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaInputStreamConsumer;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaInputStreamFactory;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetProvider;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetReader;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetReaderFromHbase;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetValidator;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetValidatorProvider;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetWriterIntoHbase;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetWriterProvider;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaStreamingContextFactory;
import com.godepth.apache.spark.kafkastreaming.spark.SparkStreamConsumer;
import com.godepth.apache.spark.kafkastreaming.spark.SparkStreamConsumerBytesToStringTranscoder;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.stream.Collectors;

public class KafkaStreamingContextFactoryUsingHbaseFactory {

    public static KafkaStreamingContextFactory<String> create(
        int batchDurationInSeconds,
        Configuration hbaseConfiguration,
        SingleKeyTableMetadata hbaseTableMetadata,
        String kafkaOffsetTopicGroupName,
        String kafkaTopic,
        int kafkaNumberOfPartitions,
        Map<String, String> kafkaProperties,
        Map<String, String> sparkProperties,
        SparkStreamConsumer<String> sparkStreamConsumer
    ) {
        JavaSparkContextFactory javaSparkContextFactory = new JavaSparkContextFactory(sparkProperties);
        KafkaInputStreamConsumer<String> kafkaInputStreamConsumer = new KafkaInputStreamConsumer<>();
        KafkaInputStreamFactory kafkaInputStreamFactory = new KafkaInputStreamFactory(kafkaProperties);
        SparkStreamConsumerBytesToStringTranscoder sparkStreamConsumerTranscoder = new SparkStreamConsumerBytesToStringTranscoder();
        ThreadSleeper threadSleeper = new ThreadSleeper();

        Map<String, String> hbaseProperties =
            IterableToStream
                .stream(hbaseConfiguration)
                .collect(
                    Collectors.toMap(
                        entry -> entry.getKey(),
                        entry -> entry.getValue()
                    )
                );

        ConnectionFactory hbaseConnectionFactory =
            new ConnectionFactory(
                hbaseProperties
            );

        KafkaOffsetReader kafkaOffsetReader =
            new KafkaOffsetReaderFromHbase(
                hbaseConnectionFactory,
                hbaseTableMetadata,
                kafkaOffsetTopicGroupName
            );

        KafkaOffsetProvider kafkaOffsetProvider =
            new KafkaOffsetProvider(
                kafkaOffsetReader,
                kafkaTopic,
                kafkaNumberOfPartitions
            );

        KafkaOffsetValidatorProvider kafkaOffsetValidatorProvider = () ->
            new KafkaOffsetValidator(
                new KafkaOffsetReaderFromHbase(
                    hbaseConnectionFactory,
                    hbaseTableMetadata,
                    kafkaOffsetTopicGroupName
                )
            );

        KafkaOffsetWriterProvider kafkaOffsetWriterProvider = () ->
            new KafkaOffsetWriterIntoHbase(
                hbaseConnectionFactory,
                hbaseTableMetadata,
                kafkaOffsetTopicGroupName,
                5,
                100,
                threadSleeper
            );

        return new KafkaStreamingContextFactory<>(
            batchDurationInSeconds,
            javaSparkContextFactory,
            kafkaInputStreamConsumer,
            kafkaInputStreamFactory,
            kafkaOffsetProvider,
            kafkaOffsetValidatorProvider,
            kafkaOffsetWriterProvider,
            () -> sparkStreamConsumer,
            sparkStreamConsumerTranscoder
        );
    }
}
