package com.github.ddth.kafka;

import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

/**
 * Represents a Kafka message.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class KafkaMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    public static Charset utf8 = Charset.forName("UTF-8");

    private String topic, key;
    private byte[] content;
    private int partition;
    private long offset;

    private String consumerGroupId;
    private long checksum;
    private long timestamp = System.currentTimeMillis();
    private TimestampType timestampType = TimestampType.NO_TIMESTAMP_TYPE;
    private int serializedKeySize, serializedContentSize;

    public KafkaMessage() {
    }

    public KafkaMessage(String topic, byte[] content) {
        topic(topic);
        content(content);
    }

    public KafkaMessage(String topic, String key, byte[] content) {
        topic(topic);
        key(key);
        content(content);
    }

    public KafkaMessage(String topic, byte[] key, byte[] content) {
        topic(topic);
        key(key);
        content(content);
    }

    public KafkaMessage(String topic, String content) {
        topic(topic);
        content(content);
    }

    public KafkaMessage(String topic, String key, String content) {
        topic(topic);
        key(key);
        content(content);
    }

    public KafkaMessage(String topic, byte[] key, String content) {
        topic(topic);
        key(key);
        content(content);
    }

    /**
     * Constructs a new {@link KafkaMessage} from a {@link ConsumerRecord}.
     * 
     * @param cr
     * @since 1.2.0
     */
    public KafkaMessage(ConsumerRecord<String, byte[]> cr) {
        topic(cr.topic());
        key(cr.key());
        content(cr.value());
        partition(cr.partition());
        offset(cr.offset());

        checksum(cr.checksum());
        timestamp(cr.timestamp());
        timestampType(cr.timestampType());
        serializedKeySize(cr.serializedKeySize());
        serializedContentSize(cr.serializedValueSize());
    }

    /**
     * Constructs a new {@link KafkaMessage} from another {@link KafkaMessage}.
     * 
     * @param another
     */
    public KafkaMessage(KafkaMessage another) {
        topic(another.topic());
        key(another.key());
        content(another.content());
        partition(another.partition());
        offset(another.offset());
    }

    public String topic() {
        return topic;
    }

    public KafkaMessage topic(String value) {
        this.topic = value;
        return this;
    }

    public String key() {
        return key;
    }

    public KafkaMessage key(byte[] value) {
        this.key = value != null ? new String(value, utf8) : null;
        return this;
    }

    public KafkaMessage key(String value) {
        this.key = value;
        return this;
    }

    public byte[] content() {
        return content;
    }

    public KafkaMessage content(byte[] value) {
        this.content = value;
        return this;
    }

    public KafkaMessage content(String value) {
        this.content = value != null ? value.getBytes(utf8) : null;
        return this;
    }

    public String contentAsString() {
        return content != null ? new String(content, utf8) : null;
    }

    public KafkaMessage partition(int value) {
        this.partition = value;
        return this;
    }

    public int partition() {
        return partition;
    }

    public KafkaMessage offset(long value) {
        this.offset = value;
        return this;
    }

    public long offset() {
        return offset;
    }

    /**
     * Checksum of the message (populated when consumed).
     * 
     * @return
     * @since 1.3.2
     */
    public long checksum() {
        return checksum;
    }

    /**
     * Checksum of the message (populated when consumed).
     * 
     * @param checksum
     * @return
     * @since 1.3.2
     */
    public KafkaMessage checksum(long checksum) {
        this.checksum = checksum;
        return this;
    }

    /**
     * GroupId of the consumer which consumed this message (populated when
     * consumed).
     * 
     * @return
     * @since 1.3.2
     */
    public String consumerGroupId() {
        return consumerGroupId;
    }

    /**
     * GroupId of the consumer which consumed this message (populated when
     * consumed).
     * 
     * @param consumerGroupId
     * @return
     * @since 1.3.2
     */
    public KafkaMessage consumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        return this;
    }

    /**
     * 
     * @return
     * @since 1.3.2
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * 
     * @param timestamp
     * @return
     * @since 1.3.2
     */
    public KafkaMessage timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * 
     * @return
     * @since 1.3.2
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * 
     * @param timestampType
     * @return
     * @since 1.3.2
     */
    public KafkaMessage timestampType(TimestampType timestampType) {
        this.timestampType = timestampType;
        return this;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null,
     * the returned size is -1 (populated when consumed).
     * 
     * @return
     * @since 1.3.2
     */
    public int serializedKeySize() {
        return serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null,
     * the returned size is -1 (populated when consumed).
     * 
     * @param serializedKeySize
     * @return
     * @since 1.3.2
     */
    public KafkaMessage serializedKeySize(int serializedKeySize) {
        this.serializedKeySize = serializedKeySize;
        return this;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is
     * null, the returned size is -1.
     * 
     * @return
     * @since 1.3.2
     */
    public int serializedContentSize() {
        return serializedContentSize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is
     * null, the returned size is -1.
     * 
     * @param serializedContentSize
     * @return
     * @since 1.3.2
     */
    public KafkaMessage serializedContentSize(int serializedContentSize) {
        this.serializedContentSize = serializedContentSize;
        return this;
    }

    /*----------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("topic", topic);
        tsb.append("key", key);
        tsb.append("content", content);
        tsb.append("partition", partition);
        tsb.append("offset", offset);
        tsb.append("groupId", consumerGroupId);
        return tsb.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(topic).append(key).append(partition).append(offset);
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof KafkaMessage) {
            KafkaMessage other = (KafkaMessage) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(topic, other.topic).append(key, other.key).append(partition, other.partition)
                    .append(offset, other.offset);
        }
        return false;
    }
}
