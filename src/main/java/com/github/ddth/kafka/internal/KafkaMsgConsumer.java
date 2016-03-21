package com.github.ddth.kafka.internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaMessage;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * A simple Kafka consumer client.
 * 
 * <p>
 * Each {@link KafkaMsgConsumer} is associated with a unique consumer-group-id.
 * </p>
 * 
 * <p>
 * One single {@link KafkaMsgConsumer} is used to consume messages from multiple
 * topics.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class KafkaMsgConsumer {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaMsgConsumer.class);

    private String consumerGroupId;
    private boolean consumeFromBeginning = false;

    /* Mapping {topic -> KafkaConsumer} */
    private ConcurrentMap<String, KafkaConsumer<String, byte[]>> topicConsumers = new ConcurrentHashMap<String, KafkaConsumer<String, byte[]>>();

    /* Mapping {topic -> BlockingQueue} */
    private ConcurrentMap<String, BlockingQueue<ConsumerRecord<String, byte[]>>> topicBuffers = new ConcurrentHashMap<String, BlockingQueue<ConsumerRecord<String, byte[]>>>();

    /* Mapping {topic -> [IKafkaMessageListener]} */
    private Multimap<String, IKafkaMessageListener> topicMsgListeners = HashMultimap.create();

    /* Mapping {topic -> KafkaMsgConsumerWorker} */
    private ConcurrentMap<String, KafkaMsgConsumerWorker> topicWorkers = new ConcurrentHashMap<String, KafkaMsgConsumerWorker>();

    private String bootstrapServers;
    private KafkaConsumer<?, ?> metadataConsumer;

    /**
     * Constructs an new {@link KafkaMsgConsumer} object.
     */
    public KafkaMsgConsumer(String bootstrapServers, String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
    }

    /**
     * Constructs an new {@link KafkaMsgConsumer} object.
     */
    public KafkaMsgConsumer(String bootstrapServers, String consumerGroupId,
            boolean consumeFromBeginning) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.consumeFromBeginning = consumeFromBeginning;
    }

    /**
     * Each Kafka consumer is associated with a consumer group id.
     * 
     * <p>
     * If two or more consumers have a same group-id, and consume messages from
     * a same topic: messages will be consumed just like a queue: no message is
     * consumed by more than one consumer. Which consumer consumes which message
     * is undetermined.
     * </p>
     * 
     * <p>
     * If two or more consumers with different group-ids, and consume messages
     * from a same topic: messages will be consumed just like publish-subscribe
     * pattern: one message is consumed by all consumers.
     * </p>
     * 
     * @return
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    /**
     * See {@link #getConsumerGroupId()}.
     * 
     * @param consumerGroupId
     * @return
     */
    public KafkaMsgConsumer setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        return this;
    }

    /**
     * Consume messages from the beginning? See {@code auto.offset.reset} option
     * at http://kafka.apache.org/08/configuration.html.
     * 
     * @return
     */
    public boolean isConsumeFromBeginning() {
        return consumeFromBeginning;
    }

    /**
     * Alias of {@link #isConsumeFromBeginning()}.
     * 
     * @return
     */
    public boolean getConsumeFromBeginning() {
        return consumeFromBeginning;
    }

    /**
     * Consume messages from the beginning? See {@code auto.offset.reset} option
     * at http://kafka.apache.org/08/configuration.html.
     * 
     * @param consumeFromBeginning
     * @return
     */
    public KafkaMsgConsumer setConsumeFromBeginning(boolean consumeFromBeginning) {
        this.consumeFromBeginning = consumeFromBeginning;
        return this;
    }

    /**
     * Initializing method.
     */
    public void init() {
        metadataConsumer = KafkaHelper.createKafkaConsumer(bootstrapServers, consumerGroupId,
                consumeFromBeginning, true, false);
    }

    /**
     * Destroying method.
     */
    public void destroy() {
        if (metadataConsumer != null) {
            try {
                metadataConsumer.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                metadataConsumer = null;
            }
        }

        // stop all workers
        for (KafkaMsgConsumerWorker worker : topicWorkers.values()) {
            try {
                worker.stopWorker();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        topicWorkers.clear();

        // clear all message listeners
        topicMsgListeners.clear();

        // close all KafkaConsumer
        for (KafkaConsumer<String, byte[]> consumer : topicConsumers.values()) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        topicConsumers.clear();
    }

    private Map<String, List<PartitionInfo>> topicInfo = null;
    private long lastTopicInfoFetched = 0;

    private Map<String, List<PartitionInfo>> getTopicInfo() {
        if (topicInfo == null || lastTopicInfoFetched + 1000 < System.currentTimeMillis()) {
            synchronized (metadataConsumer) {
                topicInfo = metadataConsumer.listTopics();
            }
            lastTopicInfoFetched = System.currentTimeMillis();
        }
        return topicInfo;
    }

    /**
     * Checks if a Kafka topic exists.
     * 
     * @param topic
     * @return
     * @since 1.2.0
     */
    public boolean topicExists(String topic) {
        Map<String, List<PartitionInfo>> topicInfo = getTopicInfo();
        return topicInfo != null && topicInfo.containsKey(topic);
    }

    /**
     * Gets number of partitions of a topic.
     * 
     * @param topic
     * @return topic's number of partitions, or {@code 0} if the topic does not
     *         exist
     * @since 1.2.0
     */
    public int getNumPartitions(String topic) {
        Map<String, List<PartitionInfo>> topicInfo = getTopicInfo();
        List<PartitionInfo> partitionInfo = topicInfo != null ? topicInfo.get(topic) : null;
        return partitionInfo != null ? partitionInfo.size() : 0;
    }

    /**
     * Seeks to the beginning of all partitions of a topic.
     * 
     * @param topic
     * @since 1.2.0
     */
    public void seekToBeginning(String topic) {
        KafkaConsumer<?, ?> consumer = _getConsumer(topic, true, true);
        KafkaHelper.seekToBeginning(consumer, topic);
    }

    /**
     * Seeks to the end of all partitions of a topic.
     * 
     * @param topic
     * @since 1.2.0
     */
    public void seekToEnd(String topic) {
        KafkaConsumer<?, ?> consumer = _getConsumer(topic, true, true);
        KafkaHelper.seekToEnd(consumer, topic);
    }

    /**
     * Prepares a consumer to consume messages from a Kafka topic.
     * 
     * @param topic
     * @param autoCommitOffset
     * @param leaderAutoRebalance
     * @since 1.2.0
     */
    private KafkaConsumer<String, byte[]> _getConsumer(String topic, boolean autoCommitOffset,
            boolean leaderAutoRebalance) {
        KafkaConsumer<String, byte[]> consumer = topicConsumers.get(topic);
        if (consumer == null) {
            consumer = KafkaHelper.createKafkaConsumer(bootstrapServers, consumerGroupId,
                    consumeFromBeginning, autoCommitOffset, autoCommitOffset);
            KafkaConsumer<String, byte[]> existingConsumer = topicConsumers.putIfAbsent(topic,
                    consumer);
            if (existingConsumer != null) {
                consumer.close();
                consumer = existingConsumer;
            }
        }
        return consumer;
    }

    /**
     * Gets a buffer to store consumed messages from a Kafka topic.
     * 
     * @param topic
     * @return
     * @since 1.2.0
     */
    private BlockingQueue<ConsumerRecord<String, byte[]>> _getBuffer(String topic) {
        BlockingQueue<ConsumerRecord<String, byte[]>> buffer = topicBuffers.get(topic);
        if (buffer == null) {
            buffer = new LinkedBlockingQueue<ConsumerRecord<String, byte[]>>();
            BlockingQueue<ConsumerRecord<String, byte[]>> existingBuffer = topicBuffers
                    .putIfAbsent(topic, buffer);
            if (existingBuffer != null) {
                buffer = existingBuffer;
            }
        }
        return buffer;
    }

    /**
     * Prepares a worker to consume messages from a Kafka topic.
     * 
     * @param topic
     * @param autoCommitOffset
     * @return
     */
    private KafkaMsgConsumerWorker _getWorker(String topic, boolean autoCommitOffset) {
        KafkaMsgConsumerWorker worker = topicWorkers.get(topic);
        if (worker == null) {
            Collection<IKafkaMessageListener> msgListeners = topicMsgListeners.get(topic);
            worker = new KafkaMsgConsumerWorker(this, topic, msgListeners);
            KafkaMsgConsumerWorker existingWorker = topicWorkers.putIfAbsent(topic, worker);
            if (existingWorker != null) {
                worker = existingWorker;
            } else {
                worker.start();
            }
        }
        return worker;
    }

    /**
     * Adds a message listener to a topic.
     * 
     * @param topic
     * @param messageListener
     * @return {@code true} if successful, {@code false} otherwise (the listener
     *         may have been added already)
     */
    public boolean addMessageListener(String topic, IKafkaMessageListener messageListener) {
        return addMessageListener(topic, messageListener, true);
    }

    /**
     * Adds a message listener to a topic.
     * 
     * @param topic
     * @param messageListener
     * @param autoCommitOffset
     * @return {@code true} if successful, {@code false} otherwise (the listener
     *         may have been added already)
     */
    public boolean addMessageListener(String topic, IKafkaMessageListener messageListener,
            boolean autoCommitOffset) {
        synchronized (topicMsgListeners) {
            if (topicMsgListeners.put(topic, messageListener)) {
                _getWorker(topic, autoCommitOffset);
                return true;
            }
        }
        return false;
    }

    /**
     * Removes a topic message listener.
     * 
     * @param topic
     * @param msgListener
     * @return {@code true} if successful, {@code false} otherwise (the topic
     *         may have no such listener added before)
     */
    public boolean removeMessageListener(String topic, IKafkaMessageListener msgListener) {
        synchronized (topicMsgListeners) {
            if (topicMsgListeners.remove(topic, msgListener)) {
                if (topicMsgListeners.get(topic).isEmpty()) {
                    // no more listener, stop worker
                    KafkaMsgConsumerWorker worker = topicWorkers.remove(topic);
                    if (worker != null) {
                        worker.stopWorker();
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Consumes one message from a topic.
     * 
     * @param topic
     * @return the consumed message or {@code null} if no message available
     */
    public KafkaMessage consume(final String topic) {
        return consume(topic, 10, TimeUnit.MILLISECONDS);
    }

    /**
     * Fetches messages from Kafka and puts into buffer.
     * 
     * @param buffer
     * @param topic
     * @param waitTime
     * @param waitTimeUnit
     */
    private void _fetch(BlockingQueue<ConsumerRecord<String, byte[]>> buffer, String topic,
            long waitTime, TimeUnit waitTimeUnit) {
        KafkaConsumer<String, byte[]> consumer = _getConsumer(topic, true, true);
        synchronized (consumer) {
            Set<String> subscription = consumer.subscription();
            if (subscription == null || subscription.size() == 0) {
                // this consumer has not subscribed to any topic yet
                if (topicExists(topic)) {
                    List<String> topics = Arrays.asList(topic);
                    consumer.subscribe(topics);
                }
            }
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                LOGGER.warn(e.getMessage(), e);
            }
            ConsumerRecords<String, byte[]> crList = consumer.poll(waitTimeUnit.toMillis(waitTime));
            if (crList != null) {
                for (ConsumerRecord<String, byte[]> cr : crList) {
                    buffer.offer(cr);
                }
            }
        }
    }

    /**
     * Consumes one message from a topic, wait up to specified wait-time.
     * 
     * @param topic
     * @param waitTime
     * @param waitTimeUnit
     * @return the consumed message or {@code null} if no message available
     */
    public KafkaMessage consume(String topic, long waitTime, TimeUnit waitTimeUnit) {
        BlockingQueue<ConsumerRecord<String, byte[]>> buffer = _getBuffer(topic);
        ConsumerRecord<String, byte[]> cr = buffer.poll();
        if (cr == null) {
            _fetch(buffer, topic, waitTime, waitTimeUnit);
            cr = buffer.poll();
        }
        return cr != null ? new KafkaMessage(cr) : null;
    }
}
