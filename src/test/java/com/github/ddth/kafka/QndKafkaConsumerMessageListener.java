package com.github.ddth.kafka;

import java.util.Random;

import kafka.admin.CreateTopicCommand;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;

public class QndKafkaConsumerMessageListener {

    public static void main(String[] args) throws InterruptedException {

        final String KAFKA_BROKER_CONNSTR = "localhost:9092";
        final String KAFKA_BROKER_ZKCONNSTR = "localhost:2181/kafka";

        Random rand = new Random(System.currentTimeMillis());
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        KafkaProducer kafkaProducer = new KafkaProducer(KAFKA_BROKER_CONNSTR);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = new KafkaConsumer(KAFKA_BROKER_ZKCONNSTR, "my-group-id");
        kafkaConsumer.init();

        String topic = "topic_test_" + rand.nextInt(timestamp);
        ZkClient zkClient = new ZkClient(KAFKA_BROKER_ZKCONNSTR, 30000, 30000,
                ZKStringSerializer$.MODULE$);
        CreateTopicCommand.createTopic(zkClient, topic, 2, 1, "");
        System.out.println(topic);
        Thread.sleep(2000);
        zkClient.close();

        kafkaConsumer.addMessageListener(topic, new IKafkaMessageListener() {
            @Override
            public void onMessage(String topic, int partition, long offset, byte[] key,
                    byte[] message) {
                System.out.print(topic + ": ");
                System.out.println(message != null ? new String(message) : null);
            }
        });
        Thread.sleep(2000);
        for (int i = 0; i < 10; i++) {
            kafkaProducer.sendMessage(topic, "message - " + i + ": " + System.currentTimeMillis());
        }
        Thread.sleep(2000);

        kafkaProducer.destroy();
        kafkaConsumer.destroy();
    }
}
