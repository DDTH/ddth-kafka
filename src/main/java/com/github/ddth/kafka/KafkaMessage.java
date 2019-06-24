package com.github.ddth.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Represents a Kafka message.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class KafkaMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public final static KafkaMessage[] EMPTY_ARRAY = new KafkaMessage[0];

    private String topic, key;
    private byte[] content;
    private int partition;
    private long offset;

    private String consumerGroupId;

    /**
     * @deprecated As of Kafka 0.11.0. Because of the potential for message format conversion on the broker, the
     * checksum returned by the broker may not match what was computed by the producer.
     * It is therefore unsafe to depend on this checksum for end-to-end delivery guarantees. Additionally,
     * message format v2 does not include a record-level checksum (for performance, the record checksum
     * was replaced with a batch checksum). To maintain compatibility, a partial checksum computed from
     * the record timestamp, serialized key size, and serialized value size is returned instead, but
     * this should not be depended on for end-to-end reliability.
     */
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

    /**
     * Kafka topic where this message was consumed from or will be published to.
     *
     * @return
     */
    public String topic() {
        return topic;
    }

    /**
     * Kafka topic where this message was consumed from or will be published to.
     *
     * @param value
     * @return
     */
    public KafkaMessage topic(String value) {
        this.topic = value;
        return this;
    }

    /**
     * Message's key.
     *
     * @return
     */
    public String key() {
        return key;
    }

    /**
     * Message's key.
     *
     * @param value
     * @return
     */
    public KafkaMessage key(byte[] value) {
        this.key = value != null ? new String(value, StandardCharsets.UTF_8) : null;
        return this;
    }

    /**
     * Message's key.
     *
     * @param value
     * @return
     */
    public KafkaMessage key(String value) {
        this.key = value;
        return this;
    }

    /**
     * Message's content.
     *
     * @return
     */
    public byte[] content() {
        return content;
    }

    /**
     * Message's content.
     *
     * @param value
     * @return
     */
    public KafkaMessage content(byte[] value) {
        this.content = value;
        return this;
    }

    /**
     * Message's content.
     *
     * @param value
     * @return
     */
    public KafkaMessage content(String value) {
        this.content = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
        return this;
    }

    /**
     * Message's content.
     *
     * @return
     */
    public String contentAsString() {
        return content != null ? new String(content, StandardCharsets.UTF_8) : null;
    }

    /**
     * Kafka topic's partition where message was consumed or published to.
     *
     * @param value
     * @return
     */
    public KafkaMessage partition(int value) {
        this.partition = value;
        return this;
    }

    /**
     * Kafka topic's partition where message was consumed or published to.
     *
     * @return
     */
    public int partition() {
        return partition;
    }

    /**
     * Message's offset within the topic partition (populated when consumed or published).
     *
     * @param value
     * @return
     */
    public KafkaMessage offset(long value) {
        this.offset = value;
        return this;
    }

    /**
     * Message's offset within the topic partition (populated when consumed or published).
     *
     * @return
     */
    public long offset() {
        return offset;
    }

    /**
     * Checksum of the message (populated when consumed).
     *
     * @return
     * @since 1.3.2
     * @deprecated As of Kafka 0.11.0. Because of the potential for message format conversion on the broker, the
     * checksum returned by the broker may not match what was computed by the producer.
     * It is therefore unsafe to depend on this checksum for end-to-end delivery guarantees. Additionally,
     * message format v2 does not include a record-level checksum (for performance, the record checksum
     * was replaced with a batch checksum). To maintain compatibility, a partial checksum computed from
     * the record timestamp, serialized key size, and serialized value size is returned instead, but
     * this should not be depended on for end-to-end reliability.
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
     * @deprecated As of Kafka 0.11.0. Because of the potential for message format conversion on the broker, the
     * checksum returned by the broker may not match what was computed by the producer.
     * It is therefore unsafe to depend on this checksum for end-to-end delivery guarantees. Additionally,
     * message format v2 does not include a record-level checksum (for performance, the record checksum
     * was replaced with a batch checksum). To maintain compatibility, a partial checksum computed from
     * the record timestamp, serialized key size, and serialized value size is returned instead, but
     * this should not be depended on for end-to-end reliability.
     */
    public KafkaMessage checksum(long checksum) {
        this.checksum = checksum;
        return this;
    }

    /**
     * Group-id of the consumer which consumed this message (populated when consumed).
     *
     * @return
     * @since 1.3.2
     */
    public String consumerGroupId() {
        return consumerGroupId;
    }

    /**
     * Group-id of the consumer which consumed this message (populated when consumed).
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
     * Message's timestamp (populated when consumed).
     *
     * @return
     * @since 1.3.2
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Message's timestamp (populated when consumed).
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
     * Message's timestamp-type (populated when consumed).
     *
     * @return
     * @since 1.3.2
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * Message's timestamp-type (populated when consumed).
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
        tsb.append("topic", topic).append("key", key).append("content", content).append("partition", partition)
                .append("offset", offset).append("groupId", consumerGroupId);
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
