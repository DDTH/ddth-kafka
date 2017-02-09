package com.github.ddth.kafka;

import java.io.Closeable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.kafka.internal.KafkaHelper;
import com.github.ddth.kafka.internal.KafkaMsgConsumer;

/**
 * A simple Kafka client (producer & consumer).
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class KafkaClient implements Closeable {

    /**
     * Producer type:
     * 
     * <ul>
     * <li>{@code FULL_ASYNC}: fully async producer (messages are sent in
     * background thread), requires no ack from broker - maximum throughput but
     * lowest durability.</li>
     * <li>{@code SYNC_NO_ACK}: sync producer, requires no ack from broker -
     * lowest latency but the weakest durability guarantees.</li>
     * <li>{@code SYNC_LEADER_ACK}: sync producer, requires ack from the leader
     * replica - balance latency/durability.</li>
     * <li>{@code SYNC_ALL_ACKS}: sync producer, requires ack from all in-sync
     * replicas - best durability.</li>
     * </ul>
     */
    public static enum ProducerType {
        FULL_ASYNC, SYNC_NO_ACK, SYNC_LEADER_ACK, SYNC_ALL_ACKS;
        public final static ProducerType[] ALL_TYPES = { FULL_ASYNC, SYNC_NO_ACK, SYNC_LEADER_ACK,
                SYNC_ALL_ACKS };
    }

    public final static ProducerType DEFAULT_PRODUCER_TYPE = ProducerType.SYNC_LEADER_ACK;

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaClient.class);

    private ExecutorService executorService;
    private boolean myOwnExecutorService = true;

    private String kafkaBootstrapServers;

    private Properties producerProperties, consumerProperties;

    private String metadataConsumerGroupId = "ddth-kafka";
    private KafkaConsumer<String, byte[]> metadataConsumer;

    /**
     * Constructs an new {@link KafkaClient} object.
     */
    public KafkaClient() {
    }

    /**
     * Constructs a new {@link KafkaClient} object with a specified Kafka
     * bootstrap server list.
     * 
     * @param kafkaBootstrapServers
     *            format "host1:port1,host2:port2,host3:port3"
     * @since 1.2.0
     */
    public KafkaClient(String kafkaBootstrapServers) {
        setKafkaBootstrapServers(kafkaBootstrapServers);
    }

    /**
     * Constructs a new {@link KafkaClient} object with a specified Kafka
     * bootstrap server list and a Executor service.
     * 
     * @param kafkaBootstrapServers
     *            format "host1:port1,host2:port2,host3:port3"
     * @param executor
     * @since 1.2.0
     */
    public KafkaClient(String kafkaBootstrapServers, ExecutorService executor) {
        setKafkaBootstrapServers(kafkaBootstrapServers);
        setExecutorService(executor);
    }

    /**
     * Group-id for the metadata consumer.
     * 
     * @return
     * @since 1.3.0
     */
    public String getMetadataConsumerGroupId() {
        return metadataConsumerGroupId;
    }

    /**
     * Sets group-id for the metadata consumer.
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
     * Sets an {@link ExecutorService} to be used for async task.
     * 
     * @param executorService
     * @return
     */
    public KafkaClient setExecutorService(ExecutorService executorService) {
        if (this.executorService != null) {
            this.executorService.shutdown();
        }
        this.executorService = executorService;
        myOwnExecutorService = false;
        return this;
    }

    /**
     * Gets custom producer configuration properties.
     * 
     * @return
     * @since 1.2.1
     */
    public Properties getProducerProperties() {
        return producerProperties;
    }

    /**
     * Sets custom producer configuration properties.
     * 
     * @param props
     * @return
     * @since 1.2.1
     */
    public KafkaClient setProducerProperties(Properties props) {
        if (props == null) {
            producerProperties = null;
        } else {
            producerProperties = new Properties();
            producerProperties.putAll(props);
        }
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
    public KafkaClient setConsumerProperties(Properties props) {
        if (props == null) {
            consumerProperties = null;
        } else {
            consumerProperties = new Properties();
            consumerProperties.putAll(props);
        }
        return this;
    }

    /**
     * Init method.
     */
    public KafkaClient init() throws Exception {
        if (executorService == null) {
            int numThreads = Math.min(Math.max(Runtime.getRuntime().availableProcessors(), 1), 4);
            executorService = Executors.newFixedThreadPool(numThreads);
            myOwnExecutorService = true;
        } else {
            myOwnExecutorService = false;
        }

        if (metadataConsumer == null) {
            metadataConsumer = KafkaHelper.createKafkaConsumer(getKafkaBootstrapServers(),
                    getMetadataConsumerGroupId(), false, true, false);
        }

        if (cacheProducers == null) {
            cacheProducers = new ConcurrentHashMap<>();
            for (ProducerType type : ProducerType.ALL_TYPES) {
                cacheProducers.put(type, KafkaHelper.createKafkaProducer(type,
                        kafkaBootstrapServers, producerProperties));
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
     * Destroying method.
     */
    @SuppressWarnings("unused")
    public void destroy() {
        if (cacheProducers != null) {
            for (Entry<ProducerType, KafkaProducer<String, byte[]>> entry : cacheProducers
                    .entrySet()) {
                try {
                    KafkaProducer<String, byte[]> producer = entry.getValue();
                    producer.close();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
            cacheProducers.clear();
            cacheProducers = null;
        }

        try {
            if (metadataConsumer != null) {
                metadataConsumer.close();
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        } finally {
            metadataConsumer = null;
        }

        if (cacheConsumers != null) {
            for (Entry<String, KafkaMsgConsumer> entry : cacheConsumers.entrySet()) {
                try {
                    KafkaMsgConsumer consumer = entry.getValue();
                    consumer.destroy();
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

    private Future<?> submitTask(Runnable task) {
        return executorService.submit(task);
    }

    /*----------------------------------------------------------------------*/
    /* ADMIN */
    /*----------------------------------------------------------------------*/
    /**
     * Checks if a Kafka topic exists.
     * 
     * @param topicName
     * @return
     * @since 1.2.0
     */
    public boolean topicExists(String topicName) {
        return getKafkaConsumer(getMetadataConsumerGroupId(), false).topicExists(topicName);
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
        List<PartitionInfo> partitionList = getPartitionInfo(topicName);
        return partitionList != null ? partitionList.size() : 0;
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
        return getJavaProducer(DEFAULT_PRODUCER_TYPE).partitionsFor(topicName);
    }

    /**
     * Gets all available topics.
     * 
     * @return
     * @since 1.3.0
     */
    public Set<String> getTopics() {
        return getKafkaConsumer(getMetadataConsumerGroupId(), false).getTopics();
    }

    /*----------------------------------------------------------------------*/
    /* CONSUMER */
    /*----------------------------------------------------------------------*/
    /* Mapping {consumer-group-id -> KafkaMsgConsumer} */
    private ConcurrentMap<String, KafkaMsgConsumer> cacheConsumers = new ConcurrentHashMap<String, KafkaMsgConsumer>();

    private KafkaMsgConsumer _newKafkaConsumer(String consumerGroupId,
            boolean consumeFromBeginning) {
        KafkaMsgConsumer kafkaConsumer = new KafkaMsgConsumer(getKafkaBootstrapServers(),
                consumerGroupId, consumeFromBeginning);
        kafkaConsumer.setConsumerProperties(consumerProperties);
        kafkaConsumer.setMetadataConsumer(metadataConsumer);
        kafkaConsumer.setExecutorService(executorService);
        kafkaConsumer.init();
        return kafkaConsumer;
    }

    /**
     * Obtains a {@link KafkaMsgConsumer} instance.
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
    private KafkaMsgConsumer getKafkaConsumer(String consumerGroupId,
            boolean consumeFromBeginning) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        if (kafkaConsumer == null) {
            kafkaConsumer = _newKafkaConsumer(consumerGroupId, consumeFromBeginning);
            KafkaMsgConsumer temp = cacheConsumers.putIfAbsent(consumerGroupId, kafkaConsumer);
            if (temp != null) {
                kafkaConsumer.destroy();
                kafkaConsumer = temp;
            }
        }
        return kafkaConsumer;
    }

    /**
     * Seeks to a specified offset.
     * 
     * @param consumerGroupId
     * @param tpo
     * @return {@code true} if the consumer has subscribed to the specified
     *         topic/partition, {@code false} otherwise.
     * @since 1.3.2
     */
    public boolean seek(String consumerGroupId, KafkaTopicPartitionOffset tpo) {
        KafkaMsgConsumer consumer = getKafkaConsumer(consumerGroupId, false);
        return consumer.seek(tpo);
    }

    /**
     * Seeks to the beginning of all assigned partitions of a topic.
     * 
     * @param consumerGroupId
     * @param topic
     * @return {@code true} if the consumer has subscribed to the specified
     *         topic, {@code false} otherwise.
     * @since 1.2.0
     */
    public boolean seekToBeginning(String consumerGroupId, String topic) {
        KafkaMsgConsumer consumer = getKafkaConsumer(consumerGroupId, false);
        return consumer.seekToBeginning(topic);
    }

    /**
     * Seeks to the end of all assigned partitions of a topic.
     * 
     * @param consumerGroupId
     * @param topic
     * @return {@code true} if the consumer has subscribed to the specified
     *         topic, {@code false} otherwise.
     * @since 1.2.0
     */
    public boolean seekToEnd(String consumerGroupId, String topic) {
        KafkaMsgConsumer consumer = getKafkaConsumer(consumerGroupId, false);
        return consumer.seekToEnd(topic);
    }

    /**
     * Consumes one message from a topic.
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
     * Consumes one message from a topic, wait up to specified wait-time.
     * 
     * @param consumerGroupId
     * @param topic
     * @param waitTime
     * @param waitTimeUnit
     * @return
     * @since 1.2.0
     */
    public KafkaMessage consumeMessage(String consumerGroupId, String topic, long waitTime,
            TimeUnit waitTimeUnit) {
        return consumeMessage(consumerGroupId, true, topic, waitTime, waitTimeUnit);
    }

    /**
     * Consumes one message from a topic.
     * 
     * <p>
     * Note: {@code consumeFromBeginning} is ignored if there is an existing
     * consumer for the {@link consumerGroupId}.
     * </p>
     * 
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param topic
     * @return
     */
    public KafkaMessage consumeMessage(String consumerGroupId, boolean consumeFromBeginning,
            String topic) {
        KafkaMsgConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
        return kafkaConsumer.consume(topic);
    }

    /**
     * Consumes one message from a topic, wait up to specified wait-time.
     * 
     * <p>
     * Note: {@code consumeFromBeginning} is ignored if there is an existing
     * consumer for the {@link consumerGroupId}.
     * </p>
     * 
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param topic
     * @param waitTime
     * @param waitTimeUnit
     * @return
     */
    public KafkaMessage consumeMessage(String consumerGroupId, boolean consumeFromBeginning,
            String topic, long waitTime, TimeUnit waitTimeUnit) {
        KafkaMsgConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
        return kafkaConsumer.consume(topic, waitTime, waitTimeUnit);
    }

    /**
     * Adds a message listener for a topic.
     * 
     * <p>
     * Note: {@code consumeFromBeginning} is ignored if there is an existing
     * consumer for the {@link consumerGroupId}.
     * </p>
     * 
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @param topic
     * @param messageListener
     * @return {@code true} if successful, {@code false} otherwise (the listener
     *         may have been added already)
     */
    public boolean addMessageListener(String consumerGroupId, boolean consumeFromBeginning,
            String topic, IKafkaMessageListener messageListener) {
        KafkaMsgConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
        return kafkaConsumer.addMessageListener(topic, messageListener);
    }

    /**
     * Removes a topic message listener.
     * 
     * @param consumerGroupId
     * @param topic
     * @param messageListener
     * @return {@code true} if successful, {@code false} otherwise (the topic
     *         may have no such listener added before)
     */
    public boolean removeMessageListener(String consumerGroupId, String topic,
            IKafkaMessageListener messageListener) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        return kafkaConsumer != null ? kafkaConsumer.removeMessageListener(topic, messageListener)
                : false;
    }

    /**
     * Commit the specified offsets for the last consumed message.
     * 
     * @param msg
     * @param groupId
     * @return {@code true} if the topic is in subscription list, {@code false}
     *         otherwise
     * @since 1.3.2
     */
    public boolean commmit(KafkaMessage msg, String groupId) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(groupId);
        return kafkaConsumer != null ? kafkaConsumer.commit(msg) : false;
    }

    /**
     * Commit the specified offsets for the last consumed message.
     * 
     * @param msg
     * @param groupId
     * @return {@code true} if the topic is in subscription list, {@code false}
     *         otherwise
     * @since 1.3.2
     */
    public boolean commmitAsync(KafkaMessage msg, String groupId) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(groupId);
        return kafkaConsumer != null ? kafkaConsumer.commitAsync(msg) : false;
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed
     * partitions.
     * 
     * @param topic
     * @param groupId
     * @return
     * @since 1.3.2
     */
    public boolean commit(String topic, String groupId) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(groupId);
        return kafkaConsumer != null ? kafkaConsumer.commit(topic) : false;
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed
     * partitions.
     * 
     * @param topic
     * @param groupId
     * @return
     * @since 1.3.2
     */
    public boolean commitAsync(String topic, String groupId) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(groupId);
        return kafkaConsumer != null ? kafkaConsumer.commitAsync(topic) : false;
    }

    /**
     * Commit the specified offsets for the specified list of topics and
     * partitions.
     * 
     * @param groupId
     * @param tpoList
     * @since 1.3.2
     */
    public void commit(String groupId, KafkaTopicPartitionOffset... tpoList) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(groupId);
        if (kafkaConsumer != null) {
            kafkaConsumer.commit(tpoList);
        }
    }

    /**
     * Commit the specified offsets for the specified list of topics and
     * partitions.
     * 
     * @param groupId
     * @param tpoList
     * @since 1.3.2
     */
    public void commitAsync(String groupId, KafkaTopicPartitionOffset... tpoList) {
        KafkaMsgConsumer kafkaConsumer = cacheConsumers.get(groupId);
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
     * Gets a Java producer of a specific type.
     * 
     * @param type
     * @return
     */
    private KafkaProducer<String, byte[]> getJavaProducer(ProducerType type) {
        return cacheProducers.get(type);
    }

    /**
     * Sends a message, with default {@link ProducerType}.
     * 
     * @param message
     * @return a copy of message filled with partition number and offset
     */
    public KafkaMessage sendMessage(KafkaMessage message) {
        return sendMessage(DEFAULT_PRODUCER_TYPE, message);
    }

    /**
     * Sends a message, specifying {@link ProducerType}.
     * 
     * @param type
     * @param message
     * @return a copy of message filled with partition number and offset
     */
    public KafkaMessage sendMessage(ProducerType type, KafkaMessage message) {
        String key = message.key();
        ProducerRecord<String, byte[]> record = StringUtils.isEmpty(key)
                ? new ProducerRecord<>(message.topic(), message.content())
                : new ProducerRecord<>(message.topic(), key, message.content());
        KafkaProducer<String, byte[]> producer = getJavaProducer(type);
        try {
            RecordMetadata metadata = producer.send(record).get();
            KafkaMessage kafkaMessage = new KafkaMessage(message);
            kafkaMessage.partition(metadata.partition());
            kafkaMessage.offset(metadata.offset());
            return kafkaMessage;
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Flushes any messages in producer queue.
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

    /**
     * Sends a message asynchronously, with default {@link ProducerType}.
     * 
     * @param message
     * @return a copy of message filled with partition number and offset
     * @since 1.3.1
     */
    public Future<KafkaMessage> sendMessageAsync(KafkaMessage message) {
        return sendMessageAsync(DEFAULT_PRODUCER_TYPE, message);
    }

    /**
     * Sends a message asynchronously, specifying {@link ProducerType}.
     * 
     * @param type
     * @param message
     * @return a copy of message filled with partition number and offset
     */
    public Future<KafkaMessage> sendMessageAsync(ProducerType type, KafkaMessage message) {
        String key = message.key();
        ProducerRecord<String, byte[]> record = StringUtils.isEmpty(key)
                ? new ProducerRecord<>(message.topic(), message.content())
                : new ProducerRecord<>(message.topic(), key, message.content());
        KafkaProducer<String, byte[]> producer = getJavaProducer(type);
        Future<RecordMetadata> fRecordMetadata = producer.send(record);
        FutureTask<KafkaMessage> result = new FutureTask<>(new Callable<KafkaMessage>() {
            @Override
            public KafkaMessage call() throws Exception {
                RecordMetadata recordMetadata = fRecordMetadata.get();
                if (recordMetadata != null) {
                    KafkaMessage kafkaMessage = new KafkaMessage(message);
                    kafkaMessage.partition(recordMetadata.partition());
                    kafkaMessage.offset(recordMetadata.offset());
                    return kafkaMessage;
                }
                throw new KafkaException("Error while sending message to broker");
            }
        });
        submitTask(result);
        return result;
    }

    /**
     * Sends a message asynchronously, with default {@link ProducerType}.
     * 
     * <p>
     * This methods returns the underlying Kafka producer's output directly to
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
     * Sends a message asynchronously, with default {@link ProducerType}.
     * 
     * <p>
     * This methods returns the underlying Kafka producer's output directly to
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
     * Sends a message asynchronously, specifying {@link ProducerType}.
     * 
     * <p>
     * This methods returns the underlying Kafka producer's output directly to
     * caller, not converting {@link RecordMetadata} to {@link KafkaMessage}.
     * </p>
     * 
     * @param type
     * @param message
     * @return
     * @since 1.3.1
     */
    public Future<RecordMetadata> sendMessageRaw(ProducerType type, KafkaMessage message) {
        return sendMessageRaw(DEFAULT_PRODUCER_TYPE, message, null);
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
    public Future<RecordMetadata> sendMessageRaw(ProducerType type, KafkaMessage message,
            Callback callback) {
        String key = message.key();
        ProducerRecord<String, byte[]> record = StringUtils.isEmpty(key)
                ? new ProducerRecord<>(message.topic(), message.content())
                : new ProducerRecord<>(message.topic(), key, message.content());
        KafkaProducer<String, byte[]> producer = getJavaProducer(type);
        return producer.send(record, callback);
    }
}
