package com.github.ddth.kafka;

import java.io.Closeable;
import java.util.Map.Entry;
import java.util.Properties;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.kafka.internal.KafkaHelper;
import com.github.ddth.kafka.internal.KafkaMsgConsumer;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

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
    }

    public final static ProducerType DEFAULT_PRODUCER_TYPE = ProducerType.SYNC_LEADER_ACK;

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaClient.class);

    private ExecutorService executorService;
    private boolean myOwnExecutorService = true;

    private String kafkaBootstrapServers;

    private Properties producerProperties, consumerProperties;

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
            int numThreads = Runtime.getRuntime().availableProcessors();
            if (numThreads < 1) {
                numThreads = 1;
            }
            if (numThreads > 4) {
                numThreads = 4;
            }
            executorService = Executors.newFixedThreadPool(numThreads);
            myOwnExecutorService = true;
        } else {
            myOwnExecutorService = false;
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
    public void destroy() {
        try {
            if (cacheJavaProducers != null) {
                cacheJavaProducers.invalidateAll();
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        } finally {
            cacheJavaProducers = null;
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
        }

        // // to fix the error
        // //
        // "thread Thread[metrics-meter-tick-thread-1...] was interrupted but is
        // still alive..."
        // // since Kafka does not shutdown Metrics registry on close.
        // Metrics.defaultRegistry().shutdown();

        if (executorService != null && myOwnExecutorService) {
            try {
                executorService.shutdownNow();
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

    // private <T> Future<T> submitTask(Runnable task, T result) {
    // return executorService.submit(task, result);
    // }
    //
    // private <T> Future<T> submitTask(Callable<T> task) {
    // return executorService.submit(task);
    // }

    /*----------------------------------------------------------------------*/
    /* CONSUMER */
    /*----------------------------------------------------------------------*/
    /* Mapping {consumer-group-id -> KafkaConsumer} */
    private ConcurrentMap<String, KafkaMsgConsumer> cacheConsumers = new ConcurrentHashMap<String, KafkaMsgConsumer>();

    private KafkaMsgConsumer _newKafkaConsumer(String consumerGroupId,
            boolean consumeFromBeginning) {
        KafkaMsgConsumer kafkaConsumer = new KafkaMsgConsumer(getKafkaBootstrapServers(),
                consumerGroupId, consumeFromBeginning);
        kafkaConsumer.setConsumerProperties(consumerProperties);
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

    private String myGroupId = "__" + this.getClass().getCanonicalName();

    /**
     * Checks if a Kafka topic exists.
     * 
     * @param topic
     * @return
     * @since 1.2.0
     */
    public boolean topicExists(String topic) {
        KafkaMsgConsumer consumer = getKafkaConsumer(myGroupId, false);
        return consumer.topicExists(topic);
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
        KafkaMsgConsumer consumer = getKafkaConsumer(myGroupId, false);
        return consumer.getNumPartitions(topic);
    }

    /**
     * Seeks to the beginning of all partitions of a topic.
     * 
     * @param consumerGroupId
     * @param topic
     * @since 1.2.0
     */
    public void seekToBeginning(String consumerGroupId, String topic) {
        KafkaMsgConsumer consumer = getKafkaConsumer(consumerGroupId, false);
        consumer.seekToBeginning(topic);
    }

    /**
     * Seeks to the end of all partitions of a topic.
     * 
     * @param consumerGroupId
     * @param topic
     * @since 1.2.0
     */
    public void seekToEnd(String consumerGroupId, String topic) {
        KafkaMsgConsumer consumer = getKafkaConsumer(consumerGroupId, false);
        consumer.seekToEnd(topic);
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
        KafkaMsgConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, true);
        return kafkaConsumer.consume(topic);
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
        KafkaMsgConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, true);
        return kafkaConsumer.consume(topic, waitTime, waitTimeUnit);
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

    // public boolean hasTopic(String topic) {
    //
    // }

    /*----------------------------------------------------------------------*/

    /*----------------------------------------------------------------------*/
    /* PRODUCER */
    /*----------------------------------------------------------------------*/
    private LoadingCache<ProducerType, KafkaProducer<String, byte[]>> cacheJavaProducers = CacheBuilder
            .newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<ProducerType, KafkaProducer<String, byte[]>>() {
                @Override
                public void onRemoval(
                        RemovalNotification<ProducerType, KafkaProducer<String, byte[]>> entry) {
                    entry.getValue().close();
                }
            }).build(new CacheLoader<ProducerType, KafkaProducer<String, byte[]>>() {
                @Override
                public KafkaProducer<String, byte[]> load(ProducerType type) throws Exception {
                    return _newJavaProducer(type);
                }
            });

    private KafkaProducer<String, byte[]> _newJavaProducer(ProducerType type) {
        return KafkaHelper.createKafkaProducer(type, kafkaBootstrapServers, producerProperties);
    }

    /**
     * Gets a Java producer of a specific type.
     * 
     * @param type
     * @return
     */
    private KafkaProducer<String, byte[]> getJavaProducer(ProducerType type) {
        try {
            return cacheJavaProducers.get(type);
        } catch (ExecutionException e) {
            throw new KafkaException(e.getCause());
        }
    }

    /**
     * Sends a message, with default {@link ProducerType}.
     * 
     * @param message
     * @return a copy of message filled with partition number and offset
     */
    public Future<KafkaMessage> sendMessage(KafkaMessage message) {
        return sendMessage(DEFAULT_PRODUCER_TYPE, message);
    }

    /**
     * Sends a message, specifying {@link ProducerType}.
     * 
     * @param type
     * @param message
     * @return a copy of message filled with partition number and offset
     */
    public Future<KafkaMessage> sendMessage(final ProducerType type, final KafkaMessage message) {
        KafkaProducer<String, byte[]> producer = getJavaProducer(type);

        String key = message.key();
        String topic = message.topic();
        byte[] value = message.content();
        ProducerRecord<String, byte[]> record = StringUtils.isEmpty(key)
                ? new ProducerRecord<String, byte[]>(topic, value)
                : new ProducerRecord<String, byte[]>(topic, key, value);
        final Future<RecordMetadata> fRecordMetadata = producer.send(record);
        FutureTask<KafkaMessage> result = new FutureTask<KafkaMessage>(
                new Callable<KafkaMessage>() {
                    @Override
                    public KafkaMessage call() throws Exception {
                        RecordMetadata recordMetadata = fRecordMetadata.get();
                        if (recordMetadata != null) {
                            KafkaMessage kafkaMessage = new KafkaMessage(message);
                            kafkaMessage.partition(recordMetadata.partition());
                            kafkaMessage.offset(recordMetadata.offset());
                            return kafkaMessage;
                        }
                        return null;
                    }
                });
        submitTask(result);
        return result;
    }
}
