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
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.kafka.internal.RandomPartitioner;
import com.github.ddth.zookeeper.ZooKeeperClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * A simple Kafka client.
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

    public final static ProducerType DEFAULT_PRODUCER_TYPE = ProducerType.SYNC_NO_ACK;

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaClient.class);

    private String zookeeperConnectString;
    private ZooKeeperClient zkClient;
    private ExecutorService executorService = Executors.newCachedThreadPool();

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
        zkClient = new ZooKeeperClient(zookeeperConnectString);
        zkClient.init();
    }

    /**
     * Destroying method.
     */
    public void destroy() {
        try {
            if (cacheProducers != null) {
                cacheProducers.invalidateAll();
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

        if (cacheConsumers != null) {
            for (Entry<String, KafkaConsumer> entry : cacheConsumers.entrySet()) {
                try {
                    entry.getValue().destroy();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
            cacheConsumers.clear();
        }

        if (executorService != null) {
            try {
                executorService.shutdownNow();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
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
    private ConcurrentMap<String, KafkaConsumer> cacheConsumers = new ConcurrentHashMap<String, KafkaConsumer>();

    private KafkaConsumer _newKafkaConsumer(String consumerGroupId, boolean consumeFromBeginning) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(this, consumerGroupId, consumeFromBeginning);
        kafkaConsumer.init();
        return kafkaConsumer;
    }

    /**
     * Obtains {@link KafkaConsumer} instance for the specified
     * consumer-group-id.
     * 
     * @param consumerGroupId
     * @return the existing {@link KafkaConsumer} or {@code null} if not exist
     */
    protected KafkaConsumer getKafkaConsumer(String consumerGroupId) {
        return cacheConsumers.get(consumerGroupId);
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
    protected KafkaConsumer getKafkaConsumer(String consumerGroupId, boolean consumeFromBeginning) {
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
     * Consumes one message from a topic, wait up to specified wait time.
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
        KafkaConsumer kafkaConsumer = getKafkaConsumer(consumerGroupId);
        return kafkaConsumer != null ? kafkaConsumer.removeMessageListener(topic, messageListener)
                : false;
    }

    /*----------------------------------------------------------------------*/

    /*----------------------------------------------------------------------*/
    /* PRODUCER */
    /*----------------------------------------------------------------------*/
    private LoadingCache<ProducerType, Producer<String, byte[]>> cacheProducers = CacheBuilder
            .newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<ProducerType, Producer<String, byte[]>>() {
                @Override
                public void onRemoval(
                        RemovalNotification<ProducerType, Producer<String, byte[]>> entry) {
                    entry.getValue().close();
                }
            }).build(new CacheLoader<ProducerType, Producer<String, byte[]>>() {
                @Override
                public Producer<String, byte[]> load(ProducerType type) throws Exception {
                    return _newProducer(type);
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
     * Creates a new producer object.
     * 
     * @param type
     * @return
     */
    private Producer<String, byte[]> _newProducer(ProducerType type) {
        Properties props = new Properties();
        String brokerList = getBrokerList();
        props.put("metadata.broker.list", brokerList);
        props.put("key.serializer.class", StringEncoder.class.getName());
        // props.put("serializer.class", StringEncoder.class.getName());
        props.put("partitioner.class", RandomPartitioner.class.getName());

        switch (type) {
        case FULL_ASYNC: {
            props.put("request.required.acks", "0");
            props.put("producer.type", "async");
            break;
        }
        case SYNC_LEADER_ACK: {
            props.put("request.required.acks", "1");
            props.put("producer.type", "sync");
            break;
        }
        case SYNC_ALL_ACKS: {
            props.put("request.required.acks", "-1");
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
        ProducerConfig config = new ProducerConfig(props);
        return new Producer<String, byte[]>(config);
    }

    /**
     * Gets a producer instance of a specific type.
     * 
     * @param type
     * @return
     */
    private Producer<String, byte[]> getProducer(ProducerType type) {
        try {
            return cacheProducers.get(type);
        } catch (ExecutionException e) {
            throw new KafkaException(e.getCause());
        }
    }

    /**
     * Sends a message, with default {@link ProducerType}.
     * 
     * @param message
     */
    public void sendMessage(KafkaMessage message) {
        sendMessage(DEFAULT_PRODUCER_TYPE, message);
    }

    /**
     * Sends a message, specifying {@link ProducerType}.
     * 
     * @param type
     * @param message
     */
    public void sendMessage(ProducerType type, KafkaMessage message) {
        Producer<String, byte[]> producer = getProducer(type);
        KeyedMessage<String, byte[]> data = message.key() != null ? new KeyedMessage<String, byte[]>(
                message.topic(), message.key(), message.content())
                : new KeyedMessage<String, byte[]>(message.topic(), message.content());
        producer.send(data);
    }

    /**
     * Sends messages, with default {@link ProducerType}.
     * 
     * @param messages
     */
    public void sendMessages(KafkaMessage[] messages) {
        sendMessages(DEFAULT_PRODUCER_TYPE, messages);
    }

    /**
     * Sends messages, specifying {@link ProducerType}.
     * 
     * @param type
     * @param messages
     */
    public void sendMessages(ProducerType type, KafkaMessage[] messages) {
        Producer<String, byte[]> producer = getProducer(type);
        List<KeyedMessage<String, byte[]>> data = new ArrayList<KeyedMessage<String, byte[]>>();
        for (KafkaMessage message : messages) {
            KeyedMessage<String, byte[]> _data = message.key() != null ? new KeyedMessage<String, byte[]>(
                    message.topic(), message.key(), message.content())
                    : new KeyedMessage<String, byte[]>(message.topic(), message.content());
            data.add(_data);
        }
        producer.send(data);
    }
    /*----------------------------------------------------------------------*/
}
