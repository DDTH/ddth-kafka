package com.github.ddth.kafka.internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaException;
import com.github.ddth.kafka.KafkaMessage;
import com.github.ddth.kafka.KafkaTopicPartitionOffset;
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

    private Properties consumerProperties;

    private ExecutorService executorService;
    private boolean myOwnExecutorService = true;

    /**
     * Constructs an new {@link KafkaMsgConsumer} object.
     * 
     * @since 1.3.0
     */
    public KafkaMsgConsumer(String bootstrapServers, String consumerGroupId,
            KafkaConsumer<?, ?> metadataConsumer) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        setMetadataConsumer(metadataConsumer);
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
     * Constructs an new {@link KafkaMsgConsumer} object.
     * 
     * @since 1.3.0
     */
    public KafkaMsgConsumer(String bootstrapServers, String consumerGroupId,
            boolean consumeFromBeginning, KafkaConsumer<?, ?> metadataConsumer) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.consumeFromBeginning = consumeFromBeginning;
        setMetadataConsumer(metadataConsumer);
    }

    /**
     * 
     * @return
     * @since 1.3.0
     */
    public KafkaConsumer<?, ?> getMetadataConsumer() {
        return metadataConsumer;
    }

    /**
     * 
     * @param metadataConsumer
     * @return
     * @since 1.3.0
     */
    public KafkaMsgConsumer setMetadataConsumer(KafkaConsumer<?, ?> metadataConsumer) {
        this.metadataConsumer = metadataConsumer;
        return this;
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
     * Gets custom consumer configuration properties.
     * 
     * @return
     * @since 1.2.1
     */
    public Properties getConsumerProperties() {
        return consumerProperties;
    }

    /**
     * Sets custom consumer configuration properties.
     * 
     * @param props
     * @return
     * @since 1.2.1
     */
    public KafkaMsgConsumer setConsumerProperties(Properties props) {
        if (props == null) {
            consumerProperties = null;
        } else {
            consumerProperties = new Properties();
            consumerProperties.putAll(props);
        }
        return this;
    }

    /**
     * Initializing method.
     */
    public void init() {
        if (executorService == null) {
            int numThreads = Math.min(Math.max(Runtime.getRuntime().availableProcessors(), 1), 4);
            executorService = Executors.newFixedThreadPool(numThreads);
            myOwnExecutorService = true;
        } else {
            myOwnExecutorService = false;
        }
    }

    /**
     * Destroying method.
     */
    @SuppressWarnings("unused")
    public void destroy() {
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
        synchronized (topicMsgListeners) {
            topicMsgListeners.clear();
        }

        // close all KafkaConsumer
        for (KafkaConsumer<String, byte[]> consumer : topicConsumers.values()) {
            synchronized (consumer) {
                try {
                    consumer.close();
                } catch (WakeupException e) {
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
        }
        topicConsumers.clear();

        if (executorService != null && myOwnExecutorService) {
            try {
                List<Runnable> tasks = executorService.shutdownNow();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                executorService = null;
            }
        }
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
     * @param topicName
     * @return
     * @since 1.2.0
     */
    public boolean topicExists(String topicName) {
        Map<String, List<PartitionInfo>> topicInfo = getTopicInfo();
        return topicInfo != null && topicInfo.containsKey(topicName);
    }

    /**
     * Gets number of partitions of a topic.
     * 
     * @param topicName
     * @return topic's number of partitions, or {@code 0} if the topic does not
     *         exist
     * @since 1.2.0
     */
    public int getNumPartitions(String topicName) {
        Map<String, List<PartitionInfo>> topicInfo = getTopicInfo();
        List<PartitionInfo> partitionInfo = topicInfo != null ? topicInfo.get(topicName) : null;
        return partitionInfo != null ? partitionInfo.size() : 0;
    }

    /**
     * Gets partition information of a topic.
     * 
     * @param topicName
     * @return list of {@link PartitionInfo} or {@code null} if topic does not
     *         exist.
     * @since 1.3.0
     */
    public List<PartitionInfo> getPartitionInfo(String topicName) {
        Map<String, List<PartitionInfo>> topicInfo = getTopicInfo();
        List<PartitionInfo> partitionInfo = topicInfo != null ? topicInfo.get(topicName) : null;
        return partitionInfo != null ? Collections.unmodifiableList(partitionInfo) : null;
    }

    /**
     * Gets all available topics.
     * 
     * @return
     * @since 1.3.0
     */
    @SuppressWarnings("unchecked")
    public Set<String> getTopics() {
        Map<String, List<PartitionInfo>> topicInfo = getTopicInfo();
        Set<String> topics = topicInfo != null ? topicInfo.keySet() : null;
        return topics != null ? Collections.unmodifiableSet(topics) : Collections.EMPTY_SET;
    }

    /**
     * Sets an {@link ExecutorService} to be used for async task.
     * 
     * @param executorService
     * @return
     * @since 1.3.1
     */
    public KafkaMsgConsumer setExecutorService(ExecutorService executorService) {
        if (this.executorService != null) {
            this.executorService.shutdown();
        }
        this.executorService = executorService;
        myOwnExecutorService = false;
        return this;
    }

    /**
     * Seeks to a specified offset.
     * 
     * @param tpo
     * @return {@code true} if the consumer has subscribed to the specified
     *         topic/partition, {@code false} otherwise.
     * @since 1.3.2
     */
    public boolean seek(KafkaTopicPartitionOffset tpo) {
        KafkaConsumer<String, byte[]> consumer = _getConsumer(tpo.topic);
        return KafkaHelper.seek(consumer, tpo);
    }

    /**
     * Seeks to the beginning of all assigned partitions of a topic.
     * 
     * @param topic
     * @return {@code true} if the consumer has subscribed to the specified
     *         topic, {@code false} otherwise.
     * @since 1.2.0
     */
    public boolean seekToBeginning(String topic) {
        KafkaConsumer<?, ?> consumer = _getConsumer(topic);
        return KafkaHelper.seekToBeginning(consumer, topic);
    }

    /**
     * Seeks to the end of all assigned partitions of a topic.
     * 
     * @param topic
     * @return {@code true} if the consumer has subscribed to the specified
     *         topic, {@code false} otherwise.
     * @since 1.2.0
     */
    public boolean seekToEnd(String topic) {
        KafkaConsumer<?, ?> consumer = _getConsumer(topic);
        return KafkaHelper.seekToEnd(consumer, topic);
    }

    /**
     * Prepares a consumer to consume messages from a Kafka topic.
     * 
     * @param topic
     * @return
     * @since 1.3.2
     */
    private KafkaConsumer<String, byte[]> _getConsumer(String topic) {
        return _getConsumer(topic, true);
    }

    /**
     * Subscribes to a topic.
     * 
     * @param consumer
     * @param topic
     * @since 1.3.2
     */
    private void _checkAndSubscribe(KafkaConsumer<?, ?> consumer, String topic) {
        synchronized (consumer) {
            Set<String> subscription = consumer.subscription();
            if (subscription == null || !subscription.contains(topic)) {
                consumer.subscribe(Arrays.asList(topic));
            }
        }
    }

    /**
     * Prepares a consumer to consume messages from a Kafka topic.
     * 
     * @param topic
     * @param autoCommitOffsets
     * @since 1.2.0
     */
    private KafkaConsumer<String, byte[]> _getConsumer(String topic, boolean autoCommitOffsets) {
        KafkaConsumer<String, byte[]> consumer = topicConsumers.get(topic);
        if (consumer == null) {
            consumer = KafkaHelper.createKafkaConsumer(bootstrapServers, consumerGroupId,
                    consumeFromBeginning, autoCommitOffsets, consumerProperties);
            KafkaConsumer<String, byte[]> existingConsumer = topicConsumers.putIfAbsent(topic,
                    consumer);
            if (existingConsumer != null) {
                consumer.close();
                consumer = existingConsumer;
            } else {
                _checkAndSubscribe(consumer, topic);
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
     * @param autoCommitOffsets
     * @return
     */
    private KafkaMsgConsumerWorker _getWorker(String topic, boolean autoCommitOffsets) {
        KafkaMsgConsumerWorker worker = topicWorkers.get(topic);
        if (worker == null) {
            Collection<IKafkaMessageListener> msgListeners = topicMsgListeners.get(topic);
            worker = new KafkaMsgConsumerWorker(this, topic, msgListeners, executorService);
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
     * @param autoCommitOffsets
     * @return {@code true} if successful, {@code false} otherwise (the listener
     *         may have been added already)
     */
    public boolean addMessageListener(String topic, IKafkaMessageListener messageListener,
            boolean autoCommitOffsets) {
        synchronized (topicMsgListeners) {
            if (topicMsgListeners.put(topic, messageListener)) {
                _getWorker(topic, autoCommitOffsets);
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
        return consume(topic, 1000, TimeUnit.MILLISECONDS);
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
        KafkaConsumer<String, byte[]> consumer = _getConsumer(topic);
        synchronized (consumer) {
            _checkAndSubscribe(consumer, topic);
            Set<String> subscription = consumer.subscription();
            ConsumerRecords<String, byte[]> crList = subscription != null
                    && subscription.contains(topic) ? consumer.poll(waitTimeUnit.toMillis(waitTime))
                            : null;
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
        return cr != null ? new KafkaMessage(cr).consumerGroupId(consumerGroupId) : null;
    }

    /**
     * Commit the specified offsets for the last consumed message.
     * 
     * @param msg
     * @return {@code true} if the topic is in subscription list, {@code false}
     *         otherwise
     * @since 1.3.2
     */
    public boolean commit(KafkaMessage msg) {
        KafkaConsumer<String, byte[]> consumer = _getConsumer(msg.topic());
        synchronized (consumer) {
            Set<String> subscription = consumer.subscription();
            if (subscription == null || !subscription.contains(msg.topic())) {
                // this consumer has not subscribed to the topic
                return false;
            } else {
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(new TopicPartition(msg.topic(), msg.partition()),
                            new OffsetAndMetadata(msg.offset() + 1));
                    consumer.commitSync(offsets);
                } catch (WakeupException e) {
                } catch (Exception e) {
                    throw new KafkaException(e);
                }
                return true;
            }
        }
    }

    /**
     * Commit the specified offsets for the last consumed message.
     * 
     * @param msg
     * @return {@code true} if the topic is in subscription list, {@code false}
     *         otherwise
     * @since 1.3.2
     */
    public boolean commitAsync(KafkaMessage msg) {
        KafkaConsumer<String, byte[]> consumer = _getConsumer(msg.topic());
        synchronized (consumer) {
            Set<String> subscription = consumer.subscription();
            if (subscription == null || !subscription.contains(msg.topic())) {
                // this consumer has not subscribed to the topic
                return false;
            } else {
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(new TopicPartition(msg.topic(), msg.partition()),
                            new OffsetAndMetadata(msg.offset() + 1));
                    consumer.commitAsync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                Exception e) {
                            if (e != null) {
                                LOGGER.error(e.getMessage(), e);
                            }
                        }
                    });
                } catch (WakeupException e) {
                } catch (Exception e) {
                    throw new KafkaException(e);
                }
                return true;
            }
        }
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed
     * partitions.
     * 
     * @param topic
     * @return {@code true} if the topic is in subscription list, {@code false}
     *         otherwise
     * @since 1.3.2
     */
    public boolean commit(String topic) {
        KafkaConsumer<String, byte[]> consumer = _getConsumer(topic);
        synchronized (consumer) {
            Set<String> subscription = consumer.subscription();
            if (subscription == null || !subscription.contains(topic)) {
                // this consumer has not subscribed to the topic
                return false;
            }
            try {
                consumer.commitSync();
            } catch (WakeupException e) {
            } catch (Exception e) {
                throw new KafkaException(e);
            }
            return true;
        }
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed
     * partitions.
     * 
     * @param topic
     * @return {@code true} if the topic is in subscription list, {@code false}
     *         otherwise
     * @since 1.3.2
     */
    public boolean commitAsync(String topic) {
        KafkaConsumer<String, byte[]> consumer = _getConsumer(topic);
        synchronized (consumer) {
            Set<String> subscription = consumer.subscription();
            if (subscription == null || !subscription.contains(topic)) {
                // this consumer has not subscribed to the topic
                return false;
            }
            try {
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                            Exception e) {
                        if (e != null) {
                            LOGGER.error(e.getMessage(), e);
                        }
                    }
                });
            } catch (WakeupException e) {
            } catch (Exception e) {
                throw new KafkaException(e);
            }
            return true;
        }
    }

    /**
     * Commit the specified offsets for the specified list of topics and
     * partitions.
     * 
     * @param tpoList
     * @since 1.3.2
     */
    public void commit(KafkaTopicPartitionOffset... tpoList) {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> topicOffsets = new HashMap<>();
        for (KafkaTopicPartitionOffset tpo : tpoList) {
            Map<TopicPartition, OffsetAndMetadata> offsets = topicOffsets.get(tpo.topic);
            if (offsets == null) {
                offsets = new HashMap<>();
                topicOffsets.put(tpo.topic, offsets);
            }
            TopicPartition tp = new TopicPartition(tpo.topic, tpo.partition);
            OffsetAndMetadata oam = new OffsetAndMetadata(tpo.offset);
            offsets.put(tp, oam);
        }
        for (Entry<String, Map<TopicPartition, OffsetAndMetadata>> entry : topicOffsets
                .entrySet()) {
            String topic = entry.getKey();
            KafkaConsumer<String, byte[]> consumer = _getConsumer(topic);
            synchronized (consumer) {
                Set<String> subscription = consumer.subscription();
                if (subscription == null || !subscription.contains(topic)) {
                    // this consumer has not subscribed to the topic
                    LOGGER.warn("Not subscribed to topic [" + topic + "] yet!");
                } else {
                    try {
                        consumer.commitSync(entry.getValue());
                    } catch (WakeupException e) {
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                }
            }
        }
    }

    /**
     * Commit the specified offsets for the specified list of topics and
     * partitions.
     * 
     * @param tpoList
     * @since 1.3.2
     */
    public void commitAsync(KafkaTopicPartitionOffset... tpoList) {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> topicOffsets = new HashMap<>();
        for (KafkaTopicPartitionOffset tpo : tpoList) {
            Map<TopicPartition, OffsetAndMetadata> offsets = topicOffsets.get(tpo.topic);
            if (offsets == null) {
                offsets = new HashMap<>();
                topicOffsets.put(tpo.topic, offsets);
            }
            TopicPartition tp = new TopicPartition(tpo.topic, tpo.partition);
            OffsetAndMetadata oam = new OffsetAndMetadata(tpo.offset);
            offsets.put(tp, oam);
        }
        for (Entry<String, Map<TopicPartition, OffsetAndMetadata>> entry : topicOffsets
                .entrySet()) {
            String topic = entry.getKey();
            KafkaConsumer<String, byte[]> consumer = _getConsumer(topic);
            synchronized (consumer) {
                Set<String> subscription = consumer.subscription();
                if (subscription == null || !subscription.contains(topic)) {
                    // this consumer has not subscribed to the topic
                    LOGGER.warn("Not subscribed to topic [" + topic + "] yet!");
                } else {
                    try {
                        consumer.commitAsync(entry.getValue(), new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                    Exception e) {
                                if (e != null) {
                                    LOGGER.error(e.getMessage(), e);
                                }
                            }
                        });
                    } catch (WakeupException e) {
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                }
            }
        }
    }

}
