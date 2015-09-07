package com.github.ddth.kafka;

import java.io.Serializable;
import java.nio.charset.Charset;

import kafka.message.MessageAndMetadata;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

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
     * Constructs a new {@link KafkaMessage} from a {@link MessageAndMetadata}
     * object.
     * 
     * @param mm
     * @since 1.1.0
     */
    public KafkaMessage(MessageAndMetadata<byte[], byte[]> mm) {
        topic(mm.topic());
        key(mm.key());
        content(mm.message());
        partition(mm.partition());
        offset(mm.offset());
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
        return tsb.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(topic).append(key).append(content);
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
            eq.append(topic, other.topic);
            eq.append(key, other.key);
            eq.append(content, other.content);
            eq.append(partition, other.partition);
            eq.append(offset, other.offset);
        }
        return false;
    }
}
