package com.github.ddth.kafka.qnd;

import java.util.Set;

import com.github.ddth.kafka.KafkaClient;

public class QndTopicInfo {
    public static void main(String[] args) throws Exception {
        try (KafkaClient kafkaClient = new KafkaClient("localhost:9092")) {
            kafkaClient.init();

            Set<String> topics = kafkaClient.getTopics();
            System.out.println("Topics: " + topics);

            System.out.println(kafkaClient.getPartitionInfo("demo"));
            System.out.println(kafkaClient.getPartitionInfo("demo1"));
            System.out.println(kafkaClient.getPartitionInfo("demo2"));
            System.out.println(kafkaClient.getPartitionInfo("kotontai"));
            System.out.println(kafkaClient.getPartitionInfo("kotontai2"));
        }
    }

}
