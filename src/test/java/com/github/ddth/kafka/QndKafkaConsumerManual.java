package com.github.ddth.kafka;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import kafka.server.KafkaServerStartable;

import org.apache.curator.test.TestingServer;

public class QndKafkaConsumerManual extends BaseQndKafka {

    // static final String KAFKA_BROKER_CONNSTR = "localhost:9092";
    // static final String KAFKA_BROKER_ZKCONNSTR = "localhost:2181/kafka";
    static final Random rand = new Random(System.currentTimeMillis());

    public void qndAsyncProducer() throws Exception {
        System.out.println("========== QND: Async Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        TestingServer zkServer = newZkServer();
        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);

        KafkaProducer kafkaProducer = newKafkaProducer(kafkaServer, KafkaProducer.Type.FULL_ASYNC);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = newKafkaConsumer(zkServer, "my-group-id");
        kafkaConsumer.init();

        // create topic
        String topic = "topic_test_" + rand.nextInt(timestamp);
        createTopic(zkServer, topic);

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
        kafkaServer.shutdown();
        zkServer.close();
    }

    protected void qndSyncNoAckProducer() throws Exception {
        System.out.println("========== QND: SyncNoAck Producer ==========");
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
        kafkaServer.shutdown();
        zkServer.close();
    }

    protected void qndSyncLeaderAckProducer() throws Exception {
        System.out.println("========== QND: SyncLeaderAck Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        TestingServer zkServer = newZkServer();
        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);

        KafkaProducer kafkaProducer = newKafkaProducer(kafkaServer,
                KafkaProducer.Type.SYNC_LEADER_ACK);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = newKafkaConsumer(zkServer, "my-group-id");
        kafkaConsumer.init();

        // create topic
        String topic = "topic_test_" + rand.nextInt(timestamp);
        createTopic(zkServer, topic);

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
        kafkaServer.shutdown();
        zkServer.close();
    }

    protected void qndSyncAllAcksProducer() throws Exception {
        System.out.println("========== QND: SyncAllAcks Producer ==========");
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        TestingServer zkServer = newZkServer();
        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);

        KafkaProducer kafkaProducer = newKafkaProducer(kafkaServer,
                KafkaProducer.Type.SYNC_ALL_ACKS);
        kafkaProducer.init();

        KafkaConsumer kafkaConsumer = newKafkaConsumer(zkServer, "my-group-id");
        kafkaConsumer.init();

        // create topic
        String topic = "topic_test_" + rand.nextInt(timestamp);
        createTopic(zkServer, topic);

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
        kafkaServer.shutdown();
        zkServer.close();
    }

    public static void main(String[] args) throws Exception {
        QndKafkaConsumerManual test = new QndKafkaConsumerManual();

        test.qndAsyncProducer();
        test.qndSyncNoAckProducer();
        test.qndSyncLeaderAckProducer();
        test.qndSyncAllAcksProducer();
    }
}
