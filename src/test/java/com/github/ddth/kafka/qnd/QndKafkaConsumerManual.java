package com.github.ddth.kafka.qnd;

import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndKafkaConsumerManual {

    private final static String brokers = "localhost:9092";
    private final static String consumerGroupId = "ddth-kafka";
    private final static String topic = "ddth-kafka";

    protected void qndNoAckProducer() throws Exception {
        System.out.println("========== QND: NoAck Producer ==========");
        try (KafkaClient kafkaClient = new KafkaClient(brokers)) {
            kafkaClient.init();

            long timestart = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                KafkaMessage msg = new KafkaMessage(topic,
                        "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(KafkaClient.ProducerType.NO_ACK, msg);
                long t1 = System.currentTimeMillis();
                msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 10000,
                        TimeUnit.MILLISECONDS);
                long t2 = System.currentTimeMillis();
                System.out.println((msg != null ? msg.contentAsString() : null) + "\t" + (t2 - t1));
            }
            long timeEnd = System.currentTimeMillis();
            System.out.println("Total: " + (timeEnd - timestart));
            Thread.sleep(2000);
        }
    }

    protected void qndLeaderAckProducer() throws Exception {
        System.out.println("========== QND: LeaderAck Producer ==========");
        try (KafkaClient kafkaClient = new KafkaClient(brokers)) {
            kafkaClient.init();

            long timestart = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                KafkaMessage msg = new KafkaMessage(topic,
                        "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(KafkaClient.ProducerType.LEADER_ACK, msg);
                long t1 = System.currentTimeMillis();
                msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 10000,
                        TimeUnit.MILLISECONDS);
                long t2 = System.currentTimeMillis();
                System.out.println((msg != null ? msg.contentAsString() : null) + "\t" + (t2 - t1));
            }
            long timeEnd = System.currentTimeMillis();
            System.out.println("Total: " + (timeEnd - timestart));
            Thread.sleep(2000);
        }
    }

    protected void qndAllAcksProducer() throws Exception {
        System.out.println("========== QND: AllAcks Producer ==========");
        try (KafkaClient kafkaClient = new KafkaClient(brokers)) {
            kafkaClient.init();

            long timestart = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                KafkaMessage msg = new KafkaMessage(topic,
                        "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(KafkaClient.ProducerType.ALL_ACKS, msg);
                long t1 = System.currentTimeMillis();
                msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 10000,
                        TimeUnit.MILLISECONDS);
                long t2 = System.currentTimeMillis();
                System.out.println((msg != null ? msg.contentAsString() : null) + "\t" + (t2 - t1));
            }
            long timeEnd = System.currentTimeMillis();
            System.out.println("Total: " + (timeEnd - timestart));
            Thread.sleep(2000);
        }
    }

    public static void main(String[] args) throws Exception {
        QndKafkaConsumerManual test = new QndKafkaConsumerManual();

        test.qndNoAckProducer();
        test.qndLeaderAckProducer();
        test.qndAllAcksProducer();
    }
}
