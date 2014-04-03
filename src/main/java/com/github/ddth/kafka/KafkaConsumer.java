package com.github.ddth.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.kafka.internal.KafkaConsumerWorker;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * A simple Kafka consumer client.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class KafkaConsumer {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    private String zookeeperConnectString;
    private String consumerGroupId;
    private boolean consumeFromBeginning = false;

    private ExecutorService executorService = Executors.newCachedThreadPool();

    /* Mapping {topic -> consumer-connector} */
    private ConcurrentMap<String, ConsumerConnector> topicConsumers = new ConcurrentHashMap<String, ConsumerConnector>();

    /* Mapping {topic -> [kafka-message-listerners]} */
    private Multimap<String, IKafkaMessageListener> topicListeners = HashMultimap.create();

    /* Mapping {topic -> [kafka-consumer-workers]} */
    private Multimap<String, KafkaConsumerWorker> topicConsumerWorkers = HashMultimap.create();

    /**
     * Constructs an new {@link KafkaConsumer} object.
     */
    public KafkaConsumer() {
    }

    /**
     * Constructs a new {@link KafkaConsumer} object with specified ZooKeeper
     * connection string and consumer group id
     * 
     * @param zookeeperConnectString
     *            format "host1:port,host2:port,host3:port" or
     *            "host1:port,host2:port,host3:port/chroot"
     * @param consumerGroupId
     * @since 0.2.1
     */
    public KafkaConsumer(String zookeeperConnectString, String consumerGroupId) {
        this.zookeeperConnectString = zookeeperConnectString;
        this.consumerGroupId = consumerGroupId;
    }

    /**
     * ZooKeeper connection string in form of {@code "host1:2182,host2:2182"} or
     * {@code "host1:2182,host2:2182/<chroot>"}.
     * 
     * @return
     * @since 0.2.1
     */
    public String getZookeeperConnectString() {
        return zookeeperConnectString;
    }

    /**
     * ZooKeeper connection string in form of {@code "host1:2182,host2:2182"} or
     * {@code "host1:2182,host2:2182/<chroot>"}.
     * 
     * @param zookeeperConnectString
     * @return
     * @since 0.2.1
     */
    public KafkaConsumer setZookeeperConnectString(String zookeeperConnectString) {
        this.zookeeperConnectString = zookeeperConnectString;
        return this;
    }

    /**
     * Each Kafka consumer is associated with a consumer group id.
     * 
     * <p>
     * If two or more comsumers have a same group id, and consume messages from
     * a same topic: messages will be consumed just like a queue: no message is
     * consumed by more than one consumer. Which consumer consumes which message
     * is undetermined.
     * </p>
     * 
     * <p>
     * If two or more comsumers with different group ids, and consume messages
     * from a same topic: messages will be consumed just like publish-subscribe
     * pattern: one message is consumed by all consumers.
     * </p>
     * 
     * @return
     * @since 0.2.1
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    /**
     * Each Kafka consumer is associated with a consumer group id.
     * 
     * <p>
     * If two or more comsumers have a same group id, and consume messages from
     * a same topic: messages will be consumed just like a queue: no message is
     * consumed by more than one consumer. Which consumer consumes which message
     * is undetermined.
     * </p>
     * 
     * <p>
     * If two or more comsumers with different group ids, and consume messages
     * from a same topic: messages will be consumed just like publish-subscribe
     * pattern: one message is consumed by all consumers.
     * </p>
     * 
     * @param consumerGroupId
     * @return
     * @since 0.2.1
     */
    public KafkaConsumer setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        return this;
    }

    /**
     * Consume messages from the beginning? See {@code auto.offset.reset} option
     * at http://kafka.apache.org/08/configuration.html.
     * 
     * @return
     * @since 0.2.1
     */
    public boolean isConsumeFromBeginning() {
        return consumeFromBeginning;
    }

    /**
     * Alias of {@link #isConsumeFromBeginning()}.
     * 
     * @return
     * @since 0.2.1
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
     * @since 0.2.1
     */
    public KafkaConsumer setConsumeFromBeginning(boolean consumeFromBeginning) {
        this.consumeFromBeginning = consumeFromBeginning;
        return this;
    }

    /**
     * Initializing method.
     */
    public void init() {
    }

    /**
     * Destroying method.
     */
    public void destroy() {
        if (executorService != null) {
            try {
                executorService.shutdownNow();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }

        Set<String> topicNames = new HashSet<String>(topicConsumers.keySet());
        for (String topic : topicNames) {
            try {
                removeConsumer(topic);
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
    }

    private static ConsumerConfig createConsumerConfig(String zookeeperConnectString,
            String consumerGroupId, boolean consumeFromBeginning) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperConnectString);
        props.put("group.id", consumerGroupId);
        props.put("zookeeper.session.timeout.ms", "600000");
        props.put("zookeeper.connection.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms", "5000");
        props.put("socket.timeout.ms", "5000");
        props.put("fetch.wait.max.ms", "2000");
        props.put("auto.offset.reset", consumeFromBeginning ? "smallest" : "largest");
        return new ConsumerConfig(props);
    }

    private ConsumerConnector _createConsumer(String topic) {
        ConsumerConfig consumerConfig = createConsumerConfig(zookeeperConnectString,
                consumerGroupId, consumeFromBeginning);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        return consumer;
    }

    private void _initConsumerWorkers(String topic, ConsumerConnector consumer) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        // TODO only 1 thread per topic?
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        for (KafkaStream<byte[], byte[]> stream : streams) {
            /* Note: Multimap.get() never returns null */
            /*
             * Note: Changes to the returned collection will update the
             * underlying multimap, and vice versa.
             */
            Collection<IKafkaMessageListener> messageListeners = topicListeners.get(topic);
            KafkaConsumerWorker worker = new KafkaConsumerWorker(stream, messageListeners);
            topicConsumerWorkers.put(topic, worker);
            executorService.submit(worker);
        }
    }

    private ConsumerConnector initConsumer(String topic) {
        ConsumerConnector consumer = topicConsumers.get(topic);
        if (consumer == null) {
            consumer = _createConsumer(topic);
            ConsumerConnector existingConsumer = topicConsumers.putIfAbsent(topic, consumer);
            if (existingConsumer != null) {
                consumer.shutdown();
                consumer = existingConsumer;
            } else {
                _initConsumerWorkers(topic, consumer);
            }
        }
        return consumer;
    }

    private void removeConsumer(String topic) {
        Collection<KafkaConsumerWorker> workers = topicConsumerWorkers.removeAll(topic);
        if (workers != null) {
            for (KafkaConsumerWorker worker : workers) {
                try {
                    worker.stop();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
        }

        @SuppressWarnings("unused")
        Collection<IKafkaMessageListener> listeners = topicListeners.removeAll(topic);

        try {
            ConsumerConnector consumer = topicConsumers.remove(topic);
            if (consumer != null) {
                consumer.shutdown();
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

    }

    /**
     * Adds a message listener for a topic.
     * 
     * @param topic
     * @param messageListener
     * @return {@code true} if successful, {@code false} otherwise (the listener
     *         may have been added already)
     */
    public boolean addMessageListener(String topic, IKafkaMessageListener messageListener) {
        synchronized (topicListeners) {
            if (!topicListeners.put(topic, messageListener)) {
                return false;
            }
            initConsumer(topic);
            return true;
        }
    }

    /**
     * Removes a topic message listener.
     * 
     * @param topic
     * @param messageListener
     * @return {@code true} if successful, {@code false} otherwise (the topic
     *         may have no such listener added before)
     */
    public boolean removeMessageListener(String topic, IKafkaMessageListener messageListener) {
        synchronized (topicListeners) {
            if (!topicListeners.remove(topic, messageListener)) {
                return false;
            }
            // Collection<IKafkaMessageListener> listeners =
            // topicListeners.get(topic);
            // if (listeners == null || listeners.size() == 0) {
            // // no more listeners, remove the consumer
            // removeConsumer(topic);
            // }
        }
        return true;
    }

    /**
     * Consumes one message from a topic.
     * 
     * <p>
     * This method blocks until message is available.
     * </p>
     * 
     * @param topic
     * @return
     * @throws InterruptedException
     */
    public byte[] consume(String topic) throws InterruptedException {
        final BlockingQueue<byte[]> buffer = new LinkedBlockingQueue<byte[]>();
        final IKafkaMessageListener listener = new AbstractKafkaMessagelistener(topic, this) {
            @Override
            public void onMessage(String topic, int partition, long offset, byte[] key,
                    byte[] message) {
                buffer.add(message);
                removeMessageListener(topic, this);
            }
        };
        addMessageListener(topic, listener);
        byte[] result = buffer.take();
        removeMessageListener(topic, listener);
        return result;
    }

    /**
     * Consumes one message from a topic, wait up to specified wait time.
     * 
     * @param topic
     * @param waitTime
     * @param waitTimeUnit
     * @return {@code null} if there is no message available
     * @throws InterruptedException
     */
    public byte[] consume(String topic, long waitTime, TimeUnit waitTimeUnit)
            throws InterruptedException {
        final BlockingQueue<byte[]> buffer = new LinkedBlockingQueue<byte[]>();
        final IKafkaMessageListener listener = new AbstractKafkaMessagelistener(topic, this) {
            @Override
            public void onMessage(String topic, int partition, long offset, byte[] key,
                    byte[] message) {
                buffer.add(message);
                removeMessageListener(topic, this);
            }
        };
        addMessageListener(topic, listener);
        byte[] result = buffer.poll(waitTime, waitTimeUnit);
        removeMessageListener(topic, listener);
        return result;
    }
}
