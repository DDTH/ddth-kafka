package com.github.ddth.kafka;

import com.github.ddth.kafka.internal.KafkaHelper;
import com.github.ddth.kafka.internal.KafkaMsgConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * A simple Kafka client (producer & consumer).
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class KafkaClient implements Closeable {
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaClient.class);

    /**
     * Producer type (Note: new Java producer is async):
     *
     * <li>{@code NO_ACK}: producer will not wait for any acknowledgment from
     * the server at all, {@code retries} configuration will not take effect.
     * Lowest latency but the weakest durability guarantees.</li>
     * <li>{@code LEADER_ACK}: leader will write the record to its local log but
     * will respond without awaiting full acknowledgement from all followers.
     * Balance latency/durability.</li>
     * <li>{@code ALL_ACKS}: leader will wait for the full set of in-sync
     * replicas to acknowledge the record. Best durability but the highest
     * latency.</li>
     */
    public enum ProducerType {NO_ACK, LEADER_ACK, ALL_ACKS;
        public final static ProducerType[] ALL_TYPES = { NO_ACK, LEADER_ACK, ALL_ACKS };}

    public final static ProducerType DEFAULT_PRODUCER_TYPE = ProducerType.LEADER_ACK;

    private ExecutorService executorService;
    private boolean myOwnExecutorService = true;

    private String kafkaBootstrapServers;
    private Properties producerProperties, consumerProperties;
    private KafkaConsumer<String, byte[]> metadataConsumer;
    private String metadataConsumerGroupId = "ddth-kafka";

    /**
     * Constructs an new {@link KafkaClient} object.
     */
    public KafkaClient() {
    }

    /**
     * Constructs a new {@link KafkaClient} object with a specified Kafka
     * bootstrap server list.
     *
     * @param kafkaBootstrapServers format "host1:port1,host2:port2,host3:port3"
     * @since 1.2.0
     */
    public KafkaClient(String kafkaBootstrapServers) {
        setKafkaBootstrapServers(kafkaBootstrapServers);
    }

    /**
     * Constructs a new {@link KafkaClient} object with a specified Kafka
     * bootstrap server list and an {@link ExecutorService}.
     *
     * @param kafkaBootstrapServers format "host1:port1,host2:port2,host3:port3"
     * @param executor
     * @since 1.2.0
     */
    public KafkaClient(String kafkaBootstrapServers, ExecutorService executor) {
        setKafkaBootstrapServers(kafkaBootstrapServers);
        setExecutorService(executor);
    }

    /**
     * Group-id for the metadata consumer (default value {@code ddth-kafka}).
     *
     * @return
     * @since 1.3.0
     */
    public String getMetadataConsumerGroupId() {
        return metadataConsumerGroupId;
    }

    /**
     * Group-id for the metadata consumer (default value {@code ddth-kafka}).
     *
     * @param metadataConsumerGroupId
     * @return
     * @since 1.3.0
     */
    public KafkaClient setMetadataConsumerGroupId(String metadataConsumerGroupId) {
        this.metadataConsumerGroupId = metadataConsumerGroupId;
        return this;
    }

    /**
     * Kafka bootstrap server list in format {@code "host:9042,host2:port2"}.
     *
     * @return
     * @since 1.2.0
     */
    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    /**
     * Kafka bootstrap server list in format {@code "host:9042,host2:port2"}.
     *
     * @param kafkaBootstrapServers
     * @return
     * @since 1.2.0
     */
    public KafkaClient setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        return this;
    }

    /**
     * Set an {@link ExecutorService} to be used for async task.
     *
     * @param executorService
     * @return
     */
    public KafkaClient setExecutorService(ExecutorService executorService) {
        if (this.executorService != null && myOwnExecutorService) {
            this.executorService.shutdown();
        }
        this.executorService = executorService;
        myOwnExecutorService = false;
        return this;
    }

    /**
     * Get custom producer configuration properties.
     *
     * @return
     * @since 1.2.1
     */
    public Properties getProducerProperties() {
        return producerProperties;
    }

    /**
     * Set custom producer configuration properties.
     *
     * @param props
     * @return
     * @since 1.2.1
     */
    public KafkaClient setProducerProperties(Properties props) {
        producerProperties = props != null ? (Properties) props.clone() : null;
        return this;
    }

    /**
     * Get custom consumer configuration properties.
     *
     * @return
     * @since 1.2.1
     */
    public Properties getConsumerProperties() {
        return consumerProperties;
    }

    /**
     * Set custom consumer configuration properties.
     *
     * @param props
     * @return
     * @since 1.2.1
     */
    public KafkaClient setConsumerProperties(Properties props) {
        consumerProperties = props != null ? (Properties) props.clone() : null;
        return this;
    }

    private boolean closed = false;

    /**
     * Init method.
     */
    public KafkaClient init() {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }

        if (executorService == null) {
            executorService = KafkaHelper.createExecutorServiceIfNull(null);
            myOwnExecutorService = true;
        } else {
            myOwnExecutorService = false;
        }

        if (metadataConsumer == null) {
            metadataConsumer = KafkaHelper.createKafkaConsumer(getKafkaBootstrapServers(), getMetadataConsumerGroupId(),
                    false /* consumeFromBeginning */, true /* autoCommitOffsets */);
        }

        if (cacheProducers == null) {
            cacheProducers = new ConcurrentHashMap<>();
            for (ProducerType type : ProducerType.ALL_TYPES) {
                cacheProducers
                        .put(type, KafkaHelper.createKafkaProducer(type, kafkaBootstrapServers, producerProperties));
            }
        }

        return this;
    }

    /**
     * Destroy method.
     *
     * @since 1.2.0
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        closed = true;

        if (cacheProducers != null) {
            for (Entry<ProducerType, KafkaProducer<String, byte[]>> entry : cacheProducers.entrySet()) {
                try {
                    entry.getValue().close(Duration.ofMillis(10000));
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
            cacheProducers.clear();
            cacheProducers = null;
        }

        try {
            if (metadataConsumer != null) {
                metadataConsumer.close(Duration.ofMillis(10000));
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        } finally {
            metadataConsumer = null;
        }

        if (cacheConsumers != null) {
            for (Entry<String, KafkaMsgConsumer> entry : cacheConsumers.entrySet()) {
                try {
                    entry.getValue().destroy();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
            cacheConsumers.clear();
            cacheConsumers = null;
        }

        // // to fix the error
        // //
        // "thread Thread[metrics-meter-tick-thread-1...] was interrupted but is
        // still alive..."
        // // since Kafka does not shutdown Metrics registry on close.
        // Metrics.defaultRegistry().shutdown();

        try {
            KafkaHelper.destroyExecutorService(myOwnExecutorService ? executorService : null);
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        } finally {
            executorService = null;
        }
    }

    private Future<?> submitTask(Runnable task) {
        return executorService.submit(task);
    }

    /*----------------------------------------------------------------------*/
    /* ADMIN */
    /*----------------------------------------------------------------------*/

    /**
     * Check if a Kafka topic exists.
     *
     * @param topicName
     * @return
     * @since 1.2.0
     */
    public boolean topicExists(String topicName) {
        return getTopics().contains(topicName);
    }

    /**
     * Get number of partitions of a topic.
     *
     * @param topicName
     * @return topic's number of partitions, or {@code 0} if the topic does not
     * exist
     * @since 1.2.0
     */
    public int getNumPartitions(String topicName) {
        List<PartitionInfo> partitionList = getPartitionInfo(topicName);
        return partitionList != null ? partitionList.size() : 0;
    }

    /**
     * Get partition information of a topic.
     *
     * @param topicName
     * @return list of {@link PartitionInfo} or {@code null} if topic does not
     * exist.
     * @since 1.3.0
     */
    public List<PartitionInfo> getPartitionInfo(String topicName) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        try {
            return metadataConsumer.partitionsFor(topicName);
        } catch (TimeoutException e) {
            LOGGER.warn(e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Get all available topics.
     *
     * @return
     * @since 1.3.0
     */
    public Set<String> getTopics() {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        return metadataConsumer.listTopics().keySet();
    }

    /*----------------------------------------------------------------------*/
    /* CONSUMER */
    /*----------------------------------------------------------------------*/
    /* Mapping {consumer-group-id -> KafkaMsgConsumer} */
    private ConcurrentMap<String, KafkaMsgConsumer> cacheConsumers = new ConcurrentHashMap<String, KafkaMsgConsumer>();

    private KafkaMsgConsumer _newKafkaConsumer(String consumerGroupId, boolean consumeFromBeginning) {
        KafkaMsgConsumer kafkaConsumer = new KafkaMsgConsumer(getKafkaBootstrapServers(), consumerGroupId,
                consumeFromBeginning);
        kafkaConsumer.setConsumerProperties(consumerProperties);
        kafkaConsumer.setMetadataConsumer(metadataConsumer);
        kafkaConsumer.setExecutorService(executorService);
        kafkaConsumer.init();
        return kafkaConsumer;
    }

    /**
     * Obtain a {@link KafkaMsgConsumer} instance.
     *
     * <p>
     * Note: The existing {@link KafkaMsgConsumer} will be returned if such
     * exists; otherwise a new {@link KafkaMsgConsumer} instance will be
     * created.
     * </p>
     *
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @return
     */
    private KafkaMsgConsumer getKafkaConsumer(String consumerGroupId, boolean consumeFromBeginning) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        return cacheConsumers.computeIfAbsent(consumerGroupId, (id) -> _newKafkaConsumer(id, consumeFromBeginning));
    }

    /**
     * Seek to a specified offset.
     *
     * @param consumerGroupId
     * @param tpo
     * @return {@code true} if the consumer has subscribed to the specified
     * topic/partition, {@code false} otherwise.
     * @since 1.3.2
     */
    public boolean seek(String consumerGroupId, KafkaTopicPartitionOffset tpo) {
        KafkaMsgConsumer consumer = getKafkaConsumer(consumerGroupId, false);
        return consumer.seek(tpo);
    }

    /**
     * Seek to the beginning of all assigned partitions of a topic.
     *
     * @param consumerGroupId
     * @param topic
     * @return {@code true} if the consumer has subscribed to the specified
     * topic, {@code false} otherwise.
     * @since 1.2.0
     */
    public boolean seekToBeginning(String consumerGroupId, String topic) {
        KafkaMsgConsumer consumer = getKafkaConsumer(consumerGroupId, false);
        return consumer.seekToBeginning(topic);
    }

    /**
     * Seek to the end of all assigned partitions of a topic.
     *
     * @param consumerGroupId
     * @param topic
     * @return {@code true} if the consumer has subscribed to the specified
     * topic, {@code false} otherwise.
     * @since 1.2.0
     */
    public boolean seekToEnd(String consumerGroupId, String topic) {
        KafkaMsgConsumer consumer = getKafkaConsumer(consumerGroupId, false);
        return consumer.seekToEnd(topic);
    }

    /**
     * Consume one message from a topic.
     *
     * @param consumerGroupId
     * @param topic
     * @return
     * @since 1.2.0
     */
    public KafkaMessage consumeMessage(String consumerGroupId, String topic) {
        return consumeMessage(consumerGroupId, true, topic);
    }

    /**
     * Consume one message from a topic, wait up to specified wait-time.
     *
     * @param consumerGroupId
     * @param topic
     * @param waitTime
     * @param waitTimeUnit
     * @return
     * @since 1.2.0
     */
    public KafkaMessage consumeMessage(String consumerGroupId, String topic, long waitTime, TimeUnit waitTimeUnit) {
        return consumeMessage(consumerGroupId, true, topic, waitTime, waitTimeUnit);
    }

    /**
     * Consume one message from a topic.
     *
     * <p>
     * Note: {@code consumeFromBeginning} is ignored if there is an existing
     * consumer for the {@code consumerGroupId}.
     * </p>
     *
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param topic
     * @return
     */
    public KafkaMessage consumeMessage(String consumerGroupId, boolean consumeFromBeginning, String topic) {
        KafkaMsgConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
        return kafkaConsumer.consume(topic);
    }

    /**
     * Consume one message from a topic, wait up to specified wait-time.
     *
     * <p>
     * Note: {@code consumeFromBeginning} is ignored if there is an existing
     * consumer for the {@code consumerGroupId}.
     * </p>
     *
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param topic
     * @param waitTime
     * @param waitTimeUnit
     * @return
     */
    public KafkaMessage consumeMessage(String consumerGroupId, boolean consumeFromBeginning, String topic,
            long waitTime, TimeUnit waitTimeUnit) {
        KafkaMsgConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
        return kafkaConsumer.consume(topic, waitTime, waitTimeUnit);
    }

    /**
     * Add a message listener for a topic.
     *
     * <p>
     * Note: {@code consumeFromBeginning} is ignored if there is an existing
     * consumer for the {@code consumerGroupId}.
     * </p>
     *
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param topic
     * @param messageListener
     * @return {@code true} if successful, {@code false} otherwise (the listener
     * may have been added already)
     */
    public boolean addMessageListener(String consumerGroupId, boolean consumeFromBeginning, String topic,
            IKafkaMessageListener messageListener) {
        KafkaMsgConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
        return kafkaConsumer.addMessageListener(topic, messageListener);
    }

    /**
     * Remove a topic message listener.
     *
     * @param consumerGroupId
     * @param topic
     * @param messageListener
     * @return {@code true} if successful, {@code false} otherwise (the topic
     * may have no such listener added before)
     */
    public boolean removeMessageListener(String consumerGroupId, String topic, IKafkaMessageListener messageListener) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        return kafkaConsumer != null ? kafkaConsumer.removeMessageListener(topic, messageListener) : false;
    }

    /**
     * Commit the specified offsets for the last consumed message.
     *
     * @param msg
     * @param consumerGroupId
     * @return {@code true} if the topic is in subscription list, {@code false}
     * otherwise
     * @since 1.3.2
     */
    public boolean commmit(KafkaMessage msg, String consumerGroupId) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        return kafkaConsumer != null ? kafkaConsumer.commit(msg) : false;
    }

    /**
     * Commit the specified offsets for the last consumed message.
     *
     * @param msg
     * @param consumerGroupId
     * @return {@code true} if the topic is in subscription list, {@code false}
     * otherwise
     * @since 1.3.2
     */
    public boolean commmitAsync(KafkaMessage msg, String consumerGroupId) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        return kafkaConsumer != null ? kafkaConsumer.commitAsync(msg) : false;
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed
     * partitions.
     *
     * @param topic
     * @param consumerGroupId
     * @return
     * @since 1.3.2
     */
    public boolean commit(String topic, String consumerGroupId) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        return kafkaConsumer != null ? kafkaConsumer.commit(topic) : false;
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed
     * partitions.
     *
     * @param topic
     * @param consumerGroupId
     * @return
     * @since 1.3.2
     */
    public boolean commitAsync(String topic, String consumerGroupId) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        return kafkaConsumer != null ? kafkaConsumer.commitAsync(topic) : false;
    }

    /**
     * Commit the specified offsets for the specified list of topics and
     * partitions.
     *
     * @param consumerGroupId
     * @param tpoList
     * @since 1.3.2
     */
    public void commit(String consumerGroupId, KafkaTopicPartitionOffset... tpoList) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        if (kafkaConsumer != null) {
            kafkaConsumer.commit(tpoList);
        }
    }

    /**
     * Commit the specified offsets for the specified list of topics and
     * partitions.
     *
     * @param consumerGroupId
     * @param tpoList
     * @since 1.3.2
     */
    public void commitAsync(String consumerGroupId, KafkaTopicPartitionOffset... tpoList) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        if (kafkaConsumer != null) {
            kafkaConsumer.commitAsync(tpoList);
        }
    }
    /*----------------------------------------------------------------------*/

    /*----------------------------------------------------------------------*/
    /* PRODUCER */
    /*----------------------------------------------------------------------*/
    /* Mapping {producer-type -> KafkaProducer} */
    private ConcurrentMap<ProducerType, KafkaProducer<String, byte[]>> cacheProducers;

    /**
     * Get a Java producer of a specific type.
     *
     * @param type
     * @return
     */
    private KafkaProducer<String, byte[]> getJavaProducer(ProducerType type) {
        if (closed) {
            throw new IllegalStateException("This KafkaClient has been closed.");
        }
        return cacheProducers.get(type);
    }

    /**
     * Flush any messages in producer queue.
     *
     * @since 1.3.1
     */
    public void flush() {
        for (ProducerType type : ProducerType.ALL_TYPES) {
            KafkaProducer<String, byte[]> producer = getJavaProducer(type);
            if (producer != null) {
                producer.flush();
            }
        }
    }

    private ProducerRecord<String, byte[]> buildProducerRecord(KafkaMessage message) {
        String key = message.key();
        return StringUtils.isEmpty(key) ?
                new ProducerRecord<>(message.topic(), message.content()) :
                new ProducerRecord<>(message.topic(), key, message.content());
    }

    private KafkaMessage buildKafkaMessageAfterSending(KafkaMessage org, RecordMetadata metadata) {
        if (metadata != null && org != null) {
            KafkaMessage kafkaMessage = new KafkaMessage(org);
            kafkaMessage.partition(metadata.partition());
            kafkaMessage.offset(metadata.offset());
            return kafkaMessage;
        }
        throw new KafkaException("Error while sending message to broker");
    }

    /**
     * Send batch of messages, with default {@link ProducerType}.
     *
     * <p>Note: messages are not sent as transaction. Some may fail while others success.</p>
     *
     * @param messages
     * @return a copy of message list filled with partition number and offset
     * @since 2.0.0
     */
    public List<KafkaMessage> sendBulk(KafkaMessage... messages) {
        return sendBulk(DEFAULT_PRODUCER_TYPE, messages);
    }

    /**
     * Send batch of messages, specifying {@link ProducerType}.
     *
     * <p>Note: messages are not sent as transaction. Some may fail while others may success.</p>
     *
     * @param type
     * @param messages
     * @return a copy of message list filled with partition number and offset
     * @since 2.0.0
     */
    public List<KafkaMessage> sendBulk(ProducerType type, KafkaMessage... messages) {
        KafkaProducer<String, byte[]> producer = getJavaProducer(type);
        List<Callable<KafkaMessage>> fTaskList = new ArrayList<>();
        for (KafkaMessage message : messages) {
            ProducerRecord<String, byte[]> record = buildProducerRecord(message);
            Future<RecordMetadata> fRecordMetadata = producer.send(record);
            fTaskList.add(() -> buildKafkaMessageAfterSending(message, fRecordMetadata.get()));
        }
        try {
            return executorService.invokeAll(fTaskList).parallelStream().map(f -> {
                try {
                    return f.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new KafkaException(e);
                }
            }).collect(Collectors.toList());
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Send a message, with default {@link ProducerType}.
     *
     * @param message
     * @return a copy of message filled with partition number and offset
     */
    public KafkaMessage sendMessage(KafkaMessage message) {
        return sendMessage(DEFAULT_PRODUCER_TYPE, message);
    }

    /**
     * Send a message, specifying {@link ProducerType}.
     *
     * @param type
     * @param message
     * @return a copy of message filled with partition number and offset
     */
    public KafkaMessage sendMessage(ProducerType type, KafkaMessage message) {
        ProducerRecord<String, byte[]> record = buildProducerRecord(message);
        KafkaProducer<String, byte[]> producer = getJavaProducer(type);
        try {
            return buildKafkaMessageAfterSending(message, producer.send(record).get());
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Send a message asynchronously, with default {@link ProducerType}.
     *
     * @param message
     * @return a copy of message filled with partition number and offset
     * @since 1.3.1
     */
    public Future<KafkaMessage> sendMessageAsync(KafkaMessage message) {
        return sendMessageAsync(DEFAULT_PRODUCER_TYPE, message);
    }

    /**
     * Send a message asynchronously, specifying {@link ProducerType}.
     *
     * @param type
     * @param message
     * @return a copy of message filled with partition number and offset
     */
    public Future<KafkaMessage> sendMessageAsync(ProducerType type, KafkaMessage message) {
        ProducerRecord<String, byte[]> record = buildProducerRecord(message);
        KafkaProducer<String, byte[]> producer = getJavaProducer(type);
        Future<RecordMetadata> fRecordMetadata = producer.send(record);
        FutureTask<KafkaMessage> result = new FutureTask<>(
                () -> buildKafkaMessageAfterSending(message, fRecordMetadata.get()));
        submitTask(result);
        return result;
    }

    /**
     * Send a message asynchronously, with default {@link ProducerType}.
     *
     * <p>
     * This method returns the underlying Kafka producer's output directly to
     * caller, not converting {@link RecordMetadata} to {@link KafkaMessage}.
     * </p>
     *
     * @param message
     * @return
     * @since 1.3.1
     */
    public Future<RecordMetadata> sendMessageRaw(KafkaMessage message) {
        return sendMessageRaw(DEFAULT_PRODUCER_TYPE, message);
    }

    /**
     * Send a message asynchronously, with default {@link ProducerType}.
     *
     * <p>
     * This method returns the underlying Kafka producer's output directly to
     * caller, not converting {@link RecordMetadata} to {@link KafkaMessage}.
     * </p>
     *
     * @param message
     * @param callback
     * @return
     * @since 1.3.1
     */
    public Future<RecordMetadata> sendMessageRaw(KafkaMessage message, Callback callback) {
        return sendMessageRaw(DEFAULT_PRODUCER_TYPE, message, callback);
    }

    /**
     * Send a message asynchronously, specifying {@link ProducerType}.
     *
     * <p>
     * This method returns the underlying Kafka producer's output directly to
     * caller, not converting {@link RecordMetadata} to {@link KafkaMessage}.
     * </p>
     *
     * @param type
     * @param message
     * @return
     * @since 1.3.1
     */
    public Future<RecordMetadata> sendMessageRaw(ProducerType type, KafkaMessage message) {
        return sendMessageRaw(type, message, null);
    }

    /**
     * Sends a message asynchronously, specifying {@link ProducerType}.
     *
     * <p>
     * This methods returns the underlying Kafka producer's output directly to
     * caller, not converting {@link RecordMetadata} to {@link KafkaMessage}.
     * </p>
     *
     * @param type
     * @param message
     * @param callback
     * @return
     * @since 1.3.1
     */
    public Future<RecordMetadata> sendMessageRaw(ProducerType type, KafkaMessage message, Callback callback) {
        ProducerRecord<String, byte[]> record = buildProducerRecord(message);
        KafkaProducer<String, byte[]> producer = getJavaProducer(type);
        return producer.send(record, callback);
    }
}
