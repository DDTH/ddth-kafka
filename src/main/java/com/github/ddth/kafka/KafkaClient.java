package com.github.ddth.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.kafka.internal.KafkaConsumer;
import com.github.ddth.zookeeper.ZooKeeperClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.yammer.metrics.Metrics;

/**
 * A simple Kafka client (producer & consumer).
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class KafkaClient {

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

    private String zookeeperConnectString;
    private ZooKeeperClient zkClient;
    private ExecutorService executorService;

    /**
     * Constructs an new {@link KafkaClient} object.
     */
    public KafkaClient() {
    }

    /**
     * Constructs a new {@link KafkaClient} object with specified ZooKeeper
     * connection string.
     * 
     * @param zookeeperConnectString
     *            format "host1:port,host2:port,host3:port" or
     *            "host1:port,host2:port,host3:port/chroot"
     */
    public KafkaClient(String zookeeperConnectString) {
        this.zookeeperConnectString = zookeeperConnectString;
    }

    /**
     * ZooKeeper connection string in format {@code "host1:2182,host2:2182"} or
     * {@code "host1:2182,host2:2182/<chroot>"}.
     * 
     * @return
     */
    public String getZookeeperConnectString() {
        return zookeeperConnectString;
    }

    /**
     * ZooKeeper connection string in format {@code "host1:2182,host2:2182"} or
     * {@code "host1:2182,host2:2182/<chroot>"}.
     * 
     * @param zookeeperConnectString
     * @return
     */
    public KafkaClient setZookeeperConnectString(String zookeeperConnectString) {
        this.zookeeperConnectString = zookeeperConnectString;
        return this;
    }

    /**
     * Initializing method.
     */
    public void init() throws Exception {
        executorService = Executors.newCachedThreadPool();

        zkClient = new ZooKeeperClient(zookeeperConnectString);
        zkClient.init();
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
            for (Entry<String, KafkaConsumer> entry : cacheConsumers.entrySet()) {
                try {
                    KafkaConsumer consumer = entry.getValue();
                    consumer.destroy();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
            cacheConsumers.clear();
        }

        // to fix the error
        // "thread Thread[metrics-meter-tick-thread-1...] was interrupted but is still alive..."
        // since Kafka does not shutdown Metrics registry on close.
        Metrics.defaultRegistry().shutdown();

        if (executorService != null) {
            try {
                executorService.shutdownNow();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                executorService = null;
            }
        }

        try {
            if (zkClient != null) {
                zkClient.destroy();
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        } finally {
            zkClient = null;
        }
    }

    public Future<?> submitTask(Runnable task) {
        return executorService.submit(task);
    }

    public <T> Future<T> submitTask(Runnable task, T result) {
        return executorService.submit(task, result);
    }

    public <T> Future<T> submitTask(Callable<T> task) {
        return executorService.submit(task);
    }

    /**
     * Gets number of partitions for a topic.
     * 
     * @param topicName
     * @return number of partitions of a topic, or {@code 0} if topic does not
     *         exist
     */
    public int getTopicNumPartitions(String topicName) {
        Object obj = zkClient.getDataJson("/brokers/topics/" + topicName);
        Map<?, ?> partitionData = DPathUtils.getValue(obj, "partitions", Map.class);
        return partitionData != null ? partitionData.size() : 0;
    }

    /*----------------------------------------------------------------------*/
    /* CONSUMER */
    /*----------------------------------------------------------------------*/
    /* Mapping {consumer-group-id -> KafkaConsumer} */
    private ConcurrentMap<String, KafkaConsumer> cacheConsumers = new ConcurrentHashMap<String, KafkaConsumer>();

    private KafkaConsumer _newKafkaConsumer(String consumerGroupId, boolean consumeFromBeginning) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(this, consumerGroupId, consumeFromBeginning);
        kafkaConsumer.init();
        return kafkaConsumer;
    }

    /**
     * Obtains a {@link KafkaConsumer} instance.
     * 
     * <p>
     * Note: The existing {@link KafkaConsumer} will be returned if such exists;
     * otherwise a new {@link KafkaConsumer} instance will be created.
     * </p>
     * 
     * @param consumerGroupId
     * @param consumeFromBeginning
     * @return
     */
    private KafkaConsumer getKafkaConsumer(String consumerGroupId, boolean consumeFromBeginning) {
        KafkaConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        if (kafkaConsumer == null) {
            kafkaConsumer = _newKafkaConsumer(consumerGroupId, consumeFromBeginning);
            KafkaConsumer temp = cacheConsumers.putIfAbsent(consumerGroupId, kafkaConsumer);
            if (temp != null) {
                kafkaConsumer.destroy();
                kafkaConsumer = temp;
            }
        }
        return kafkaConsumer;
    }

    /**
     * Consumes one message from a topic.
     * 
     * <p>
     * Note: this method blocks until a message arrives.
     * </p>
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
     * @throws InterruptedException
     */
    public KafkaMessage consumeMessage(String consumerGroupId, boolean consumeFromBeginning,
            String topic) throws InterruptedException {
        KafkaConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
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
     * @throws InterruptedException
     */
    public KafkaMessage consumeMessage(String consumerGroupId, boolean consumeFromBeginning,
            String topic, long waitTime, TimeUnit waitTimeUnit) throws InterruptedException {
        KafkaConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
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
        KafkaConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId, consumeFromBeginning);
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
        KafkaConsumer kafkaConsumer = cacheConsumers.get(consumerGroupId);
        return kafkaConsumer != null ? kafkaConsumer.removeMessageListener(topic, messageListener)
                : false;
    }

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

    /**
     * Gets broker list in format {@code "host1:port1,host2:port2,..."}
     * 
     * @return
     */
    public String getBrokerList() {
        String[] children = zkClient.getChildren("/brokers/ids");
        List<String> hostsAndPorts = new ArrayList<String>();
        for (String child : children) {
            Object nodeData = zkClient.getDataJson("/brokers/ids/" + child);
            String host = DPathUtils.getValue(nodeData, "host", String.class);
            String port = DPathUtils.getValue(nodeData, "port", String.class);
            hostsAndPorts.add(host + ":" + port);
        }
        return StringUtils.join(hostsAndPorts, ',');
    }

    /**
     * Creates a new Java producer object.
     * 
     * @param type
     * @return
     */
    private KafkaProducer<String, byte[]> _newJavaProducer(ProducerType type) {
        Properties producerConfigs = new Properties();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList());
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        // producerConfigs.put("partitioner.class",
        // RandomPartitioner.class.getName());

        // 4mb buffer
        producerConfigs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 4 * 1024 * 1024);
        producerConfigs.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true");
        // ack timeout 10 seconds
        producerConfigs.put(ProducerConfig.TIMEOUT_CONFIG, "10000");
        // metadata fetch timeout: 10 seconds
        producerConfigs.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, "10000");

        /*
         * As of Kafka 0.8.2 the Java producer seems not supporting
         * "producer.type"!
         */
        switch (type) {
        case FULL_ASYNC: {
            producerConfigs.put(ProducerConfig.ACKS_CONFIG, "0");
            // producerConfigs.put("request.required.acks", "0");
            producerConfigs.put("producer.type", "async");
            break;
        }
        case SYNC_LEADER_ACK: {
            producerConfigs.put(ProducerConfig.ACKS_CONFIG, "1");
            // producerConfigs.put("request.required.acks", "1");
            producerConfigs.put("producer.type", "sync");
            break;
        }
        case SYNC_ALL_ACKS: {
            producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
            // producerConfigs.put("request.required.acks", "-1");
            producerConfigs.put("producer.type", "sync");
            break;
        }
        case SYNC_NO_ACK:
        default: {
            producerConfigs.put("request.required.acks", "0");
            producerConfigs.put("producer.type", "sync");
            break;
        }
        }
        return new KafkaProducer<String, byte[]>(producerConfigs);
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
        ProducerRecord<String, byte[]> record = StringUtils.isEmpty(key) ? new ProducerRecord<String, byte[]>(
                topic, value) : new ProducerRecord<String, byte[]>(topic, key, value);
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
