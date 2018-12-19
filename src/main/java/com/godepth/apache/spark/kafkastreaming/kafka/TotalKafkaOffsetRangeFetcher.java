package com.godepth.apache.spark.kafkastreaming.kafka;

import com.godepth.apache.spark.kafkastreaming.ThreadSleeper;
import com.godepth.apache.spark.kafkastreaming.spark.KafkaOffsetRange;
import kafka.api.PartitionMetadata;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.TopicMetadata;
import kafka.api.TopicMetadataResponse;
import kafka.client.ClientUtils;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class TotalKafkaOffsetRangeFetcher implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TotalKafkaOffsetRangeFetcher.class);

    private final String kafkaServers;
    private final ThreadSleeper threadSleeper;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final Set<String> monitoringTopics = new HashSet<>();
    private final Set<Pair<TopicAndPartition, Pair<Long, Long>>> topicAndPartitionOffsets = new HashSet<>();

    public TotalKafkaOffsetRangeFetcher(
        String kafkaServers,
        ThreadSleeper threadSleeper
    ) {
        this.kafkaServers = kafkaServers;
        this.threadSleeper = threadSleeper;
    }

    public Optional<KafkaOffsetRange> fetch(
        String topic,
        int partition
    ) {
        synchronized (monitoringTopics) {

            monitoringTopics
                .add(topic);
        }

        if (!isRunning
            .getAndSet(true)) {

            startBackgroundMonitor();
        }

        synchronized (topicAndPartitionOffsets) {

            return
                topicAndPartitionOffsets
                    .stream()
                    .filter(p -> p.getLeft().topic().equals(topic))
                    .filter(p -> p.getLeft().partition() == partition)
                    .map(p ->
                        new KafkaOffsetRange(
                            p.getLeft().topic(),
                            p.getLeft().partition(),
                            p.getRight().getKey(),
                            p.getRight().getValue()
                        )
                    )
                    .findAny();
        }
    }

    private void startBackgroundMonitor() {

        new Thread(() -> {

            while (true) {

                try {

                    Set<String> monitoringTopicsThreadSafeCopy;

                    synchronized (monitoringTopics) {

                        monitoringTopicsThreadSafeCopy = new HashSet<>(monitoringTopics);
                    }

                    Instant queryStart = Instant.now();

                    Set<Pair<TopicAndPartition, Pair<Long, Long>>> currentTopicAndPartitionOffsets =
                        fetchTopicAndPartitionOffsets(
                            kafkaServers,
                            monitoringTopicsThreadSafeCopy
                        );

                    Instant queryEnd = Instant.now();

                    LOGGER.info("@@ Kafka offsets found in " + (queryEnd.toEpochMilli() - queryStart.toEpochMilli()) + "ms");

                    synchronized (topicAndPartitionOffsets) {

                        topicAndPartitionOffsets.clear();
                        topicAndPartitionOffsets.addAll(currentTopicAndPartitionOffsets);
                    }

                } catch (RuntimeException e) {

                    LOGGER.warn(
                        "@@ Kafka offsets could not be found",
                        e
                    );
                }

                threadSleeper
                    .sleep(1000);
            }
        }).start();
    }

    private static Set<Pair<TopicAndPartition, Pair<Long, Long>>> fetchTopicAndPartitionOffsets(
        String kafkaServers,
        Set<String> topics
    ) {
        Set<Pair<TopicAndPartition, Pair<Long, Long>>> topicAndPartitionOffsets = new HashSet<>();

        String clientId = "whatever";

        Seq<BrokerEndPoint> metadataTargetBrokers = ClientUtils.parseBrokerList(kafkaServers);

        TopicMetadataResponse topicsMetadataResponse =
            ClientUtils
                .fetchTopicMetadata(
                    JavaConverters.asScalaSetConverter(topics).asScala(),
                    metadataTargetBrokers,
                    clientId,
                    1000,
                    1000
                );

        List<TopicMetadata> topicsMetadata = JavaConverters.asJavaListConverter(topicsMetadataResponse.topicsMetadata()).asJava();

        topicsMetadata
            .forEach(topicsMetadataElement -> {

                List<PartitionMetadata> partitionsMetadata = JavaConverters.asJavaListConverter(topicsMetadataElement.partitionsMetadata().seq()).asJava();

                partitionsMetadata
                    .forEach(partitionsMetadataElement -> {

                        String topic = topicsMetadataElement.topic();
                        int partition = partitionsMetadataElement.partitionId();

                        if (partitionsMetadataElement.leader().isEmpty()) {

                            LOGGER.warn(
                                "@@ Topic " + topic + "," +
                                " partition " + partition +
                                " leader not found"
                            );
                        }

                        BrokerEndPoint leader = partitionsMetadataElement.leader().get();

                        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

                        SimpleConsumer consumer = null;

                        try {

                            consumer = new SimpleConsumer(leader.host(), leader.port(), 10000, 100000, clientId);

                            long fromOffset;
                            {
                                Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
                                    Collections.singletonMap(topicAndPartition, new PartitionOffsetRequestInfo(-2, 1));

                                OffsetRequest offsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
                                OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

                                fromOffset = offsetResponse.offsets(topic, partition)[0];
                            }

                            long untilOffset;
                            {
                                Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
                                    Collections.singletonMap(topicAndPartition, new PartitionOffsetRequestInfo(-1, 1));

                                OffsetRequest offsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
                                OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

                                untilOffset = offsetResponse.offsets(topic, partition)[0];
                            }

                            topicAndPartitionOffsets
                                .add(Pair.of(topicAndPartition, Pair.of(fromOffset, untilOffset)));

                        } finally {

                            if (consumer != null) {
                                consumer.close();
                            }
                        }
                    });
            });

        return topicAndPartitionOffsets;
    }
}
