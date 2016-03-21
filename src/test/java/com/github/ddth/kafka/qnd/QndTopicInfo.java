package com.github.ddth.kafka.qnd;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

public class QndTopicInfo {
    public static void main(String[] args) {
        final String TOPIC = "demo";
        final String GROUP_ID = "mygroupid";

        Properties props = _buildConsumerProps("localhost:9092", GROUP_ID, false, true, true);
        System.out.println(props);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        Map<String, List<PartitionInfo>> topicList = consumer.listTopics();
        for (Entry<String, List<PartitionInfo>> topicInfo : topicList.entrySet()) {
            System.out.println(topicInfo.getKey());
            System.out.println(topicInfo.getValue());
        }

        System.out.println(consumer.partitionsFor("demo"));
        System.out.println(consumer.partitionsFor("kotontai"));
        System.out.println(consumer.partitionsFor("kotontai2"));
    }

    private static Properties _buildConsumerProps(String kafkaBootstrapServers,
            String consumerGroupId, boolean consumeFromBeginning, boolean autoCommitOffset,
            boolean leaderAutoRebalance) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
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

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumeFromBeginning ? "earliest"
                : "latest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

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

        return props;
    }
}
