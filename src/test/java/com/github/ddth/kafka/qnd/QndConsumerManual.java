package com.github.ddth.kafka.qnd;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerManual {

    private final static Random rand = new Random(System.currentTimeMillis());
    private final static int NUM_MSGS = 1000;

    public void qndAsyncProducer() throws Exception {
        System.out.println("========== QND: Async Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        final String BOOTSTRAP_SERVERS = "localhost:9092";
        // final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;
        final KafkaClient.ProducerType PRODUCER_TYPE = KafkaClient.ProducerType.FULL_ASYNC;

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();

            final String TOPIC = "topic_test_" + rand.nextInt(timestamp);

            long timestart = System.currentTimeMillis();
            long RECEIVED_MSGS = 0;
            for (int i = 0; i < NUM_MSGS; i++) {
                KafkaMessage msg = new KafkaMessage(TOPIC,
                        "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(PRODUCER_TYPE, msg);
                // long t1 = System.currentTimeMillis();
                msg = kafkaClient.consumeMessage(GROUP_ID, CONSUME_FROM_BEGINNING, TOPIC, 10000,
                        TimeUnit.MILLISECONDS);
                // long t2 = System.currentTimeMillis();
                if (msg != null) {
                    RECEIVED_MSGS++;
                }
                // System.out.println((msg != null ? msg.contentAsString() :
                // null) + "\t" + (t2 - t1));
            }
            long timeEnd = System.currentTimeMillis();
            System.out.println("Total: " + (timeEnd - timestart) + " / " + RECEIVED_MSGS);
            Thread.sleep(2000);
        }
    }

    public void qndSyncNoAckProducer() throws Exception {
        System.out.println("========== QND: SyncNoAck Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        final String BOOTSTRAP_SERVERS = "localhost:9092";
        // final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;
        final KafkaClient.ProducerType PRODUCER_TYPE = KafkaClient.ProducerType.SYNC_NO_ACK;

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();

            final String TOPIC = "topic_test_" + rand.nextInt(timestamp);

            long timestart = System.currentTimeMillis();
            long RECEIVED_MSGS = 0;
            for (int i = 0; i < NUM_MSGS; i++) {
                KafkaMessage msg = new KafkaMessage(TOPIC,
                        "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(PRODUCER_TYPE, msg);
                // long t1 = System.currentTimeMillis();
                msg = kafkaClient.consumeMessage(GROUP_ID, CONSUME_FROM_BEGINNING, TOPIC, 10000,
                        TimeUnit.MILLISECONDS);
                // long t2 = System.currentTimeMillis();
                if (msg != null) {
                    RECEIVED_MSGS++;
                }
                // System.out.println((msg != null ? msg.contentAsString() :
                // null) + "\t" + (t2 - t1));
            }
            long timeEnd = System.currentTimeMillis();
            System.out.println("Total: " + (timeEnd - timestart) + " / " + RECEIVED_MSGS);
            Thread.sleep(2000);
        }
    }

    public void qndSyncLeaderAckProducer() throws Exception {
        System.out.println("========== QND: SyncLeaderAck Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        final String BOOTSTRAP_SERVERS = "localhost:9092";
        // final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;
        final KafkaClient.ProducerType PRODUCER_TYPE = KafkaClient.ProducerType.SYNC_LEADER_ACK;

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();

            final String TOPIC = "topic_test_" + rand.nextInt(timestamp);

            long timestart = System.currentTimeMillis();
            long RECEIVED_MSGS = 0;
            for (int i = 0; i < NUM_MSGS; i++) {
                KafkaMessage msg = new KafkaMessage(TOPIC,
                        "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(PRODUCER_TYPE, msg);
                // long t1 = System.currentTimeMillis();
                msg = kafkaClient.consumeMessage(GROUP_ID, CONSUME_FROM_BEGINNING, TOPIC, 10000,
                        TimeUnit.MILLISECONDS);
                // long t2 = System.currentTimeMillis();
                if (msg != null) {
                    RECEIVED_MSGS++;
                }
                // System.out.println((msg != null ? msg.contentAsString() :
                // null) + "\t" + (t2 - t1));
            }
            long timeEnd = System.currentTimeMillis();
            System.out.println("Total: " + (timeEnd - timestart) + " / " + RECEIVED_MSGS);
            Thread.sleep(2000);
        }
    }

    public void qndSyncAllAcksProducer() throws Exception {
        System.out.println("========== QND: SyncAllAcks Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        final String BOOTSTRAP_SERVERS = "localhost:9092";
        // final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;
        final KafkaClient.ProducerType PRODUCER_TYPE = KafkaClient.ProducerType.SYNC_ALL_ACKS;

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();

            final String TOPIC = "topic_test_" + rand.nextInt(timestamp);

            long timestart = System.currentTimeMillis();
            long RECEIVED_MSGS = 0;
            for (int i = 0; i < NUM_MSGS; i++) {
                KafkaMessage msg = new KafkaMessage(TOPIC,
                        "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(PRODUCER_TYPE, msg);
                // long t1 = System.currentTimeMillis();
                msg = kafkaClient.consumeMessage(GROUP_ID, CONSUME_FROM_BEGINNING, TOPIC, 10000,
                        TimeUnit.MILLISECONDS);
                // long t2 = System.currentTimeMillis();
                if (msg != null) {
                    RECEIVED_MSGS++;
                }
                // System.out.println((msg != null ? msg.contentAsString() :
                // null) + "\t" + (t2 - t1));
            }
            long timeEnd = System.currentTimeMillis();
            System.out.println("Total: " + (timeEnd - timestart) + " / " + RECEIVED_MSGS);
            Thread.sleep(2000);
        }
    }

    public static void main(String[] args) throws Exception {
        QndConsumerManual test = new QndConsumerManual();

        test.qndAsyncProducer();
        test.qndSyncNoAckProducer();
        test.qndSyncLeaderAckProducer();
        test.qndSyncAllAcksProducer();
    }
}
