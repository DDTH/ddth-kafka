package com.github.ddth.kafka;

import java.util.Random;

import kafka.server.KafkaServerStartable;

import org.apache.curator.test.TestingServer;

public class QndKafkaConsumerMessageListener extends BaseQndKafka {

    public void qndMessageListener() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        TestingServer zkServer = newZkServer();
        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);

        KafkaProducer kafkaProducer = newKafkaProducer(kafkaServer, KafkaProducer.Type.SYNC_NO_ACK);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = newKafkaConsumer(zkServer, "my-group-id");
        kafkaConsumer.init();

        // create topic
        String topic = "topic_test_" + rand.nextInt(timestamp);
        createTopic(zkServer, topic);

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
        kafkaServer.shutdown();
        zkServer.close();
    }

    public static void main(String[] args) throws Exception {
        QndKafkaConsumerMessageListener test = new QndKafkaConsumerMessageListener();
        test.qndMessageListener();
    }
}
