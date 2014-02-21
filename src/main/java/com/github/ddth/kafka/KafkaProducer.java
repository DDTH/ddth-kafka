package com.github.ddth.kafka;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

/**
 * A simple Kafka producer client.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class KafkaProducer {

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
    public static enum Type {
        FULL_ASYNC, SYNC_NO_ACK, SYNC_LEADER_ACK, SYNC_ALL_ACKS
    }

    private String brokerList;
    private Type type = Type.SYNC_NO_ACK;
    private Producer<String, byte[]> producer;

    /**
     * Constructs an new {@link KafkaProducer} object.
     */
    public KafkaProducer() {
    }

    /**
     * Constructs a new {@link KafkaProducer} object with specified broker list.
     * 
     * @param brokerList
     *            format "host1:port,host2:port,host3:port"
     */
    public KafkaProducer(String brokerList) {
        this.brokerList = brokerList;
    }

    /**
     * Constructs a new {@link KafkaProducer} object with specified broker list.
     * 
     * @param brokerList
     *            format "host1:port,host2:port,host3:port"
     * @param type
     */
    public KafkaProducer(String brokerList, Type type) {
        this.brokerList = brokerList;
        this.type = type;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Initializing method.
     * 
     * <p>
     * Call this method once before sending any message.
     * </p>
     */
    public void init() {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("key.serializer.class", StringEncoder.class.getName());
        // props.put("serializer.class", StringEncoder.class.getName());
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
        producer = new Producer<String, byte[]>(config);
    }

    /**
     * Destroying method.
     * 
     * <p>
     * After calling this method, the producer is no longer usable.
     * </p>
     */
    public void destroy() {
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * Sends a message to a topic.
     * 
     * @param topic
     * @param message
     */
    public void sendMessage(String topic, byte[] message) {
        sendMessage(topic, null, message);
    }

    /**
     * Sends a message to a topic, with specified key.
     * 
     * @param topic
     * @param key
     * @param message
     */
    public void sendMessage(String topic, String key, byte[] message) {
        KeyedMessage<String, byte[]> data = key != null ? new KeyedMessage<String, byte[]>(topic,
                key, message) : new KeyedMessage<String, byte[]>(topic, message);
        producer.send(data);
    }

    /**
     * Sends a message to a topic.
     * 
     * @param topic
     * @param message
     */
    public void sendMessage(String topic, String message) {
        sendMessage(topic, null, message);
    }

    /**
     * Sends a message to a topic, with specified key.
     * 
     * @param topic
     * @param key
     * @param message
     */
    public boolean sendMessage(String topic, String key, String message) {
        try {
            byte[] byteMessage = message.getBytes("UTF-8");
            KeyedMessage<String, byte[]> data = key != null ? new KeyedMessage<String, byte[]>(
                    topic, key, byteMessage) : new KeyedMessage<String, byte[]>(topic, byteMessage);
            producer.send(data);
            return true;
        } catch (FailedToSendMessageException e) {
            return false;
        } catch (UnsupportedEncodingException e) {
            return false;
        }
    }

    /**
     * Sends messages to a topic.
     * 
     * @param topic
     * @param messages
     */
    public void sendMessages(String topic, byte[][] messages) {
        List<KeyedMessage<String, byte[]>> data = new ArrayList<KeyedMessage<String, byte[]>>();
        for (byte[] message : messages) {
            KeyedMessage<String, byte[]> _data = new KeyedMessage<String, byte[]>(topic, message);
            data.add(_data);
        }
        producer.send(data);
    }

    /**
     * Sends messages to a topic.
     * 
     * @param topic
     * @param messages
     */
    public void sendMessages(String topic, String[] messages) {
        List<KeyedMessage<String, byte[]>> data = new ArrayList<KeyedMessage<String, byte[]>>();
        for (String message : messages) {
            try {
                byte[] byteMessage = message.getBytes("UTF-8");
                KeyedMessage<String, byte[]> _data = new KeyedMessage<String, byte[]>(topic,
                        byteMessage);
                data.add(_data);
            } catch (UnsupportedEncodingException e) {
            }
        }
        producer.send(data);
    }
}
