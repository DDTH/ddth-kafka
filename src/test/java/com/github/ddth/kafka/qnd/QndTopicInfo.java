package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.KafkaClient;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public class QndTopicInfo {
    public static void main(String[] args) throws Exception {
        try (KafkaClient kafkaClient = new KafkaClient("localhost:9092")) {
            Properties pr = new Properties();
            pr.setProperty("auto.create.topics.enable", "false");
            kafkaClient.setProducerProperties(pr);
            kafkaClient.init();

            Set<String> topics = kafkaClient.getTopics();
            System.out.println("Topics: " + topics);

            for (String topic : topics) {
                System.out.println(kafkaClient.topicExists(topic) + " - Partitions for [" + topic + "]:");
                List<PartitionInfo> partitionInfoList = kafkaClient.getPartitionInfo(topic);
                for (PartitionInfo partitionInfo : partitionInfoList) {
                    System.out.println("\t" + partitionInfo);
                }
            }
            System.out.println(
                    kafkaClient.topicExists("kotontai") + " - Partitions for [" + "kotontai" + "]: " + kafkaClient
                            .getPartitionInfo("kotontai"));
            System.out.println(
                    kafkaClient.topicExists("kotontai2") + " - Partitions for [" + "kotontai2" + "]: " + kafkaClient
                            .getPartitionInfo("kotontai2"));
        }
    }
}
