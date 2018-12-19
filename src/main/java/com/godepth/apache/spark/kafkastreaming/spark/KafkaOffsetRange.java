package com.godepth.apache.spark.kafkastreaming.spark;

import org.apache.spark.streaming.kafka.OffsetRange;

import java.io.Serializable;

public class KafkaOffsetRange implements Serializable {

    private final String topic;
    private final int partition;
    private final long fromOffset;
    private final long untilOffset;

    public KafkaOffsetRange(
        OffsetRange offsetRange
    ) {
        this(
            offsetRange.topic(),
            offsetRange.partition(),
            offsetRange.fromOffset(),
            offsetRange.untilOffset()
        );
    }

    public KafkaOffsetRange(
        String topic,
        int partition,
        long fromOffset,
        long untilOffset
    ) {
        this.topic = topic;
        this.partition = partition;
        this.fromOffset = fromOffset;
        this.untilOffset = untilOffset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getCount() {
        return getUntilOffset() - getFromOffset();
    }

    public long getFromOffset() {
        return fromOffset;
    }

    public long getUntilOffset() {
        return untilOffset;
    }
}
