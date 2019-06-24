package com.github.ddth.kafka;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Encapsulates topic, partition, and offset info.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.3.2
 */
public class KafkaTopicPartitionOffset {
    public final String topic;
    public final int partition;
    public final long offset;

    public KafkaTopicPartitionOffset(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("topic", topic).append("partition", partition).append("offset", offset);
        return tsb.build();
    }
}
