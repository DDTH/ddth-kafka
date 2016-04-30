package com.github.ddth.kafka.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.github.ddth.kafka.KafkaClient.ProducerType;

/**
 * Helper utility class.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.2.0
 */
public class KafkaHelper {

    /**
     * Seeks the consumer's cursor to the beginning of a topic.
     * 
     * @param consumer
     * @param topic
     */
    public static void seekToBeginning(final KafkaConsumer<?, ?> consumer, final String topic) {
        synchronized (consumer) {
            Set<String> subscription = consumer.subscription();
            try {
                consumer.unsubscribe();
                List<PartitionInfo> partInfo = consumer.partitionsFor(topic);
                for (PartitionInfo p : partInfo) {
                    TopicPartition tp = new TopicPartition(topic, p.partition());
                    consumer.assign(Arrays.asList(tp));
                    consumer.seekToBeginning(tp);
                    consumer.position(tp);
                    consumer.commitSync();
                }
            } finally {
                consumer.unsubscribe();
                List<String> topics = new ArrayList<String>();
                topics.addAll(subscription);
                consumer.subscribe(topics);
            }
        }
    }

    /**
     * Seeks the consumer's cursor to the end of a topic.
     * 
     * @param consumer
     * @param topic
     */
    public static void seekToEnd(final KafkaConsumer<?, ?> consumer, final String topic) {
        synchronized (consumer) {
            Set<String> subscription = consumer.subscription();
            try {
                consumer.unsubscribe();
                List<PartitionInfo> partInfo = consumer.partitionsFor(topic);
                for (PartitionInfo p : partInfo) {
                    TopicPartition tp = new TopicPartition(topic, p.partition());
                    consumer.assign(Arrays.asList(tp));
                    consumer.seekToEnd(tp);
                    consumer.position(tp);
                    consumer.commitSync();
                }
            } finally {
                consumer.unsubscribe();
                List<String> topics = new ArrayList<String>();
                topics.addAll(subscription);
                consumer.subscribe(topics);
            }
        }
    }

    /**
     * Creates a new {@link KafkaProducer} instance, with default
     * configurations.
     * 
     * @param type
     * @param bootstrapServers
     * @return
     */
    public static KafkaProducer<String, byte[]> createKafkaProducer(final ProducerType type,
            final String bootstrapServers) {
        return createKafkaProducer(type, bootstrapServers, null);
    }

    /**
     * Creates a new {@link KafkaProducer} instance, with custom configuration
     * properties.
     * 
     * <p>
     * Note: custom configuration properties will be populated <i>after</i> and
     * <i>additional/overridden</i> to the default configuration.
     * </p>
     * 
     * @param type
     * @param bootstrapServers
     * @param customProps
     * @return
     * @since 1.2.1
     */
    public static KafkaProducer<String, byte[]> createKafkaProducer(final ProducerType type,
            final String bootstrapServers, Properties customProps) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());

        // 4mb buffer & 1024-record batch size
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 4 * 1024 * 1024);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);

        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // ack timeout 10 seconds
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);

        // metadata fetch timeout: 10 seconds
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);

        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        switch (type) {
        case FULL_ASYNC: {
            props.put(ProducerConfig.ACKS_CONFIG, "0");
            props.put("producer.type", "async");
            break;
        }
        case SYNC_LEADER_ACK: {
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put("producer.type", "sync");
            break;
        }
        case SYNC_ALL_ACKS: {
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put("producer.type", "sync");
            break;
        }
        case SYNC_NO_ACK:
        default: {
            props.put("request.required.acks", "0");
            props.put("producer.type", "sync");
            break;
        }
        }

        if (customProps != null) {
            // populate custom configurations
            props.putAll(customProps);
        }

        return new KafkaProducer<String, byte[]>(props);
    }

    /**
     * Creates a new {@link KafkaConsumer} instance, with default
     * configurations.
     * 
     * @param bootstrapServers
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param autoCommitOffset
     * @param leaderAutoRebalance
     * @return
     */
    public static KafkaConsumer<String, byte[]> createKafkaConsumer(final String bootstrapServers,
            final String consumerGroupId, final boolean consumeFromBeginning,
            boolean autoCommitOffset, boolean leaderAutoRebalance) {
        return createKafkaConsumer(bootstrapServers, consumerGroupId, consumeFromBeginning,
                autoCommitOffset, leaderAutoRebalance, null);
    }

    /**
     * Creates a new {@link KafkaConsumer} instance, with custom configuration
     * properties.
     * 
     * <p>
     * Note: custom configuration properties will be populated <i>after</i> and
     * <i>additional/overridden</i> to the default configuration.
     * </p>
     * 
     * @param bootstrapServers
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param autoCommitOffset
     * @param leaderAutoRebalance
     * @param customProps
     * @return
     * @since 1.2.1
     */
    public static KafkaConsumer<String, byte[]> createKafkaConsumer(final String bootstrapServers,
            final String consumerGroupId, final boolean consumeFromBeginning,
            boolean autoCommitOffset, boolean leaderAutoRebalance, Properties customProps) {
        Properties props = KafkaHelper.buildKafkaConsumerProps(bootstrapServers, consumerGroupId,
                consumeFromBeginning, autoCommitOffset, leaderAutoRebalance, customProps);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        return consumer;
    }

    /**
     * Builds default consumer's properties.
     * 
     * @param bootstrapServers
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param autoCommitOffset
     * @param leaderAutoRebalance
     * @return
     */
    public static Properties buildKafkaConsumerProps(final String bootstrapServers,
            final String consumerGroupId, final boolean consumeFromBeginning,
            final boolean autoCommitOffset, final boolean leaderAutoRebalance) {
        return buildKafkaConsumerProps(bootstrapServers, consumerGroupId, consumeFromBeginning,
                autoCommitOffset, leaderAutoRebalance, null);
    }

    /**
     * Builds default consumer's properties, and applies custom configurations
     * if any.
     * 
     * <p>
     * Note: custom configuration properties will be populated <i>after</i> and
     * <i>additional/overridden</i> to the default configuration.
     * </p>
     * 
     * @param bootstrapServers
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param autoCommitOffset
     * @param leaderAutoRebalance
     * @param customProps
     * @since 1.2.1
     * @return
     */
    public static Properties buildKafkaConsumerProps(final String bootstrapServers,
            final String consumerGroupId, final boolean consumeFromBeginning,
            final boolean autoCommitOffset, final boolean leaderAutoRebalance,
            final Properties customProps) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 3000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 7000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 9000);

        if (autoCommitOffset) {
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        } else {
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                consumeFromBeginning ? "earliest" : "latest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        if (leaderAutoRebalance) {
            props.put("auto.leader.rebalance.enable", true);
            props.put("rebalance.backoff.ms", 10000);
            props.put("refresh.leader.backoff.ms", 1000);
        } else {
            props.put("auto.leader.rebalance.enable", false);
        }

        props.put("controlled.shutdown.enable", true);
        props.put("controlled.shutdown.max.retries", 3);
        props.put("controlled.shutdown.retry.backoff.ms", 3000);

        // max 64kb
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 64 * 1024);

        if (customProps != null) {
            // populate custom configurations
            props.putAll(customProps);
        }

        return props;
    }
}
