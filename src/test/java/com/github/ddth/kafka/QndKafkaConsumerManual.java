package com.github.ddth.kafka;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import kafka.admin.CreateTopicCommand;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;

public class QndKafkaConsumerManual {

    static final String KAFKA_BROKER_CONNSTR = "localhost:9092";
    static final String KAFKA_BROKER_ZKCONNSTR = "localhost:2181/kafka";
    static final Random rand = new Random(System.currentTimeMillis());

    static void createTopic(String topic) throws InterruptedException {
        ZkClient zkClient = new ZkClient(KAFKA_BROKER_ZKCONNSTR, 30000, 30000,
                ZKStringSerializer$.MODULE$);
        CreateTopicCommand.createTopic(zkClient, topic, 2, 1, "");
        System.out.println(topic);
        Thread.sleep(2000);
        zkClient.close();
    }

    static void qndAsyncProducer() throws InterruptedException {
        System.out.println("========== QND: Async Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        KafkaProducer kafkaProducer = new KafkaProducer(KAFKA_BROKER_CONNSTR,
                KafkaProducer.Type.FULL_ASYNC);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = new KafkaConsumer(KAFKA_BROKER_ZKCONNSTR, "my-group-id");
        kafkaConsumer.init();

        // create topic
        String topic = "topic_test_" + rand.nextInt(timestamp);
        createTopic(topic);

        // call consume method once to initialize message consumer
        kafkaConsumer.consume(topic, 1000, TimeUnit.MILLISECONDS);
        long timestart = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            kafkaProducer.sendMessage(topic, "message - " + i + ": " + System.currentTimeMillis());
            long t1 = System.currentTimeMillis();
            byte[] msg = kafkaConsumer.consume(topic, 10000, TimeUnit.MILLISECONDS);
            long t2 = System.currentTimeMillis();
            String msgStr = msg != null ? new String(msg) : null;
            System.out.println(msgStr + "\t" + (t2 - t1));
        }
        long timeEnd = System.currentTimeMillis();
        System.out.println("Total: " + (timeEnd - timestart));
        Thread.sleep(2000);

        kafkaProducer.destroy();
        kafkaConsumer.destroy();
    }

    static void qndSyncNoAckProducer() throws InterruptedException {
        System.out.println("========== QND: SyncNoAck Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        KafkaProducer kafkaProducer = new KafkaProducer(KAFKA_BROKER_CONNSTR,
                KafkaProducer.Type.SYNC_NO_ACK);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = new KafkaConsumer(KAFKA_BROKER_ZKCONNSTR, "my-group-id");
        kafkaConsumer.init();

        // create topic
        String topic = "topic_test_" + rand.nextInt(timestamp);
        createTopic(topic);

        // call consume method once to initialize message consumer
        kafkaConsumer.consume(topic, 1000, TimeUnit.MILLISECONDS);
        long timestart = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            kafkaProducer.sendMessage(topic, "message - " + i + ": " + System.currentTimeMillis());
            long t1 = System.currentTimeMillis();
            byte[] msg = kafkaConsumer.consume(topic, 10000, TimeUnit.MILLISECONDS);
            long t2 = System.currentTimeMillis();
            String msgStr = msg != null ? new String(msg) : null;
            System.out.println(msgStr + "\t" + (t2 - t1));
        }
        long timeEnd = System.currentTimeMillis();
        System.out.println("Total: " + (timeEnd - timestart));
        Thread.sleep(2000);

        kafkaProducer.destroy();
        kafkaConsumer.destroy();
    }

    static void qndSyncLeaderAckProducer() throws InterruptedException {
        System.out.println("========== QND: SyncLeaderAck Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        KafkaProducer kafkaProducer = new KafkaProducer(KAFKA_BROKER_CONNSTR,
                KafkaProducer.Type.SYNC_LEADER_ACK);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = new KafkaConsumer(KAFKA_BROKER_ZKCONNSTR, "my-group-id");
        kafkaConsumer.init();

        // create topic
        String topic = "topic_test_" + rand.nextInt(timestamp);
        createTopic(topic);

        // call consume method once to initialize message consumer
        kafkaConsumer.consume(topic, 1000, TimeUnit.MILLISECONDS);
        long timestart = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            kafkaProducer.sendMessage(topic, "message - " + i + ": " + System.currentTimeMillis());
            long t1 = System.currentTimeMillis();
            byte[] msg = kafkaConsumer.consume(topic, 10000, TimeUnit.MILLISECONDS);
            long t2 = System.currentTimeMillis();
            String msgStr = msg != null ? new String(msg) : null;
            System.out.println(msgStr + "\t" + (t2 - t1));
        }
        long timeEnd = System.currentTimeMillis();
        System.out.println("Total: " + (timeEnd - timestart));
        Thread.sleep(2000);

        kafkaProducer.destroy();
        kafkaConsumer.destroy();
    }

    static void qndSyncAllAcksProducer() throws InterruptedException {
        System.out.println("========== QND: SyncAllAcks Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        KafkaProducer kafkaProducer = new KafkaProducer(KAFKA_BROKER_CONNSTR,
                KafkaProducer.Type.SYNC_ALL_ACKS);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = new KafkaConsumer(KAFKA_BROKER_ZKCONNSTR, "my-group-id");
        kafkaConsumer.init();

        // create topic
        String topic = "topic_test_" + rand.nextInt(timestamp);
        createTopic(topic);

        // call consume method once to initialize message consumer
        kafkaConsumer.consume(topic, 1000, TimeUnit.MILLISECONDS);
        long timestart = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            kafkaProducer.sendMessage(topic, "message - " + i + ": " + System.currentTimeMillis());
            long t1 = System.currentTimeMillis();
            byte[] msg = kafkaConsumer.consume(topic, 10000, TimeUnit.MILLISECONDS);
            long t2 = System.currentTimeMillis();
            String msgStr = msg != null ? new String(msg) : null;
            System.out.println(msgStr + "\t" + (t2 - t1));
        }
        long timeEnd = System.currentTimeMillis();
        System.out.println("Total: " + (timeEnd - timestart));
        Thread.sleep(2000);

        kafkaProducer.destroy();
        kafkaConsumer.destroy();
    }

    public static void main(String[] args) throws InterruptedException {
        qndAsyncProducer();
        qndSyncNoAckProducer();
        qndSyncLeaderAckProducer();
        qndSyncAllAcksProducer();
    }
}
