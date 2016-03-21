//package com.github.ddth.kafka.qnd;
//
//import java.util.Random;
//import java.util.concurrent.TimeUnit;
//
//import kafka.server.KafkaServerStartable;
//
//import org.apache.curator.test.TestingServer;
//
//import com.github.ddth.kafka.KafkaClient;
//import com.github.ddth.kafka.KafkaMessage;
//
//public class QndKafkaConsumerManual extends BaseQndKafka {
//
//    private static final Random rand = new Random(System.currentTimeMillis());
//    private final static int NUM_MSGS = 10;
//
//    public void qndAsyncProducer() throws Exception {
//        System.out.println("========== QND: Async Producer ==========");
//        int timestamp = (int) (System.currentTimeMillis() / 1000);
//
//        TestingServer zkServer = newZkServer();
//        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);
//
//        KafkaClient kafkaClient = newKafkaClient(zkServer);
//        final String consumerGroupId = "my-group-id";
//
//        // create topic
//        String topic = "topic_test_" + rand.nextInt(timestamp);
//        createTopic(zkServer, topic);
//
//        long timestart = System.currentTimeMillis();
//        for (int i = 0; i < 10; i++) {
//            KafkaMessage msg = new KafkaMessage(topic, "message - " + i + ": "
//                    + System.currentTimeMillis());
//            kafkaClient.sendMessage(KafkaClient.ProducerType.FULL_ASYNC, msg);
//            long t1 = System.currentTimeMillis();
//            msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 10000,
//                    TimeUnit.MILLISECONDS);
//            long t2 = System.currentTimeMillis();
//            System.out.println((msg != null ? msg.contentAsString() : null) + "\t" + (t2 - t1));
//        }
//        long timeEnd = System.currentTimeMillis();
//        System.out.println("Total: " + (timeEnd - timestart));
//        Thread.sleep(2000);
//
//        kafkaClient.destroy();
//
//        kafkaServer.shutdown();
//        zkServer.stop();
//        zkServer.close();
//    }
//
//    protected void qndSyncNoAckProducer() throws Exception {
//        System.out.println("========== QND: SyncNoAck Producer ==========");
//        int timestamp = (int) (System.currentTimeMillis() / 1000);
//
//        TestingServer zkServer = newZkServer();
//        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);
//
//        KafkaClient kafkaClient = newKafkaClient(zkServer);
//        final String consumerGroupId = "my-group-id";
//
//        // create topic
//        String topic = "topic_test_" + rand.nextInt(timestamp);
//        createTopic(zkServer, topic);
//
//        long timestart = System.currentTimeMillis();
//        for (int i = 0; i < 10; i++) {
//            KafkaMessage msg = new KafkaMessage(topic, "message - " + i + ": "
//                    + System.currentTimeMillis());
//            kafkaClient.sendMessage(KafkaClient.ProducerType.SYNC_NO_ACK, msg);
//            long t1 = System.currentTimeMillis();
//            msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 10000,
//                    TimeUnit.MILLISECONDS);
//            long t2 = System.currentTimeMillis();
//            System.out.println((msg != null ? msg.contentAsString() : null) + "\t" + (t2 - t1));
//        }
//        long timeEnd = System.currentTimeMillis();
//        System.out.println("Total: " + (timeEnd - timestart));
//        Thread.sleep(2000);
//
//        kafkaClient.destroy();
//
//        kafkaServer.shutdown();
//        zkServer.stop();
//        zkServer.close();
//    }
//
//    protected void qndSyncLeaderAckProducer() throws Exception {
//        System.out.println("========== QND: SyncLeaderAck Producer ==========");
//        int timestamp = (int) (System.currentTimeMillis() / 1000);
//
//        TestingServer zkServer = newZkServer();
//        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);
//
//        KafkaClient kafkaClient = newKafkaClient(zkServer);
//        final String consumerGroupId = "my-group-id";
//
//        // create topic
//        String topic = "topic_test_" + rand.nextInt(timestamp);
//        createTopic(zkServer, topic);
//
//        long timestart = System.currentTimeMillis();
//        for (int i = 0; i < 10; i++) {
//            KafkaMessage msg = new KafkaMessage(topic, "message - " + i + ": "
//                    + System.currentTimeMillis());
//            kafkaClient.sendMessage(KafkaClient.ProducerType.SYNC_LEADER_ACK, msg);
//            long t1 = System.currentTimeMillis();
//            msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 10000,
//                    TimeUnit.MILLISECONDS);
//            long t2 = System.currentTimeMillis();
//            System.out.println((msg != null ? msg.contentAsString() : null) + "\t" + (t2 - t1));
//        }
//        long timeEnd = System.currentTimeMillis();
//        System.out.println("Total: " + (timeEnd - timestart));
//        Thread.sleep(2000);
//
//        kafkaClient.destroy();
//
//        kafkaServer.shutdown();
//        zkServer.stop();
//        zkServer.close();
//    }
//
//    protected void qndSyncAllAcksProducer() throws Exception {
//        System.out.println("========== QND: SyncAllAcks Producer ==========");
//        int timestamp = (int) (System.currentTimeMillis() / 1000);
//
//        TestingServer zkServer = newZkServer();
//        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);
//
//        KafkaClient kafkaClient = newKafkaClient(zkServer);
//        final String consumerGroupId = "my-group-id";
//
//        // create topic
//        String topic = "topic_test_" + rand.nextInt(timestamp);
//        createTopic(zkServer, topic);
//
//        long timestart = System.currentTimeMillis();
//        for (int i = 0; i < 10; i++) {
//            KafkaMessage msg = new KafkaMessage(topic, "message - " + i + ": "
//                    + System.currentTimeMillis());
//            kafkaClient.sendMessage(KafkaClient.ProducerType.SYNC_ALL_ACKS, msg);
//            long t1 = System.currentTimeMillis();
//            msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 10000,
//                    TimeUnit.MILLISECONDS);
//            long t2 = System.currentTimeMillis();
//            System.out.println((msg != null ? msg.contentAsString() : null) + "\t" + (t2 - t1));
//        }
//        long timeEnd = System.currentTimeMillis();
//        System.out.println("Total: " + (timeEnd - timestart));
//        Thread.sleep(2000);
//
//        kafkaClient.destroy();
//
//        kafkaServer.shutdown();
//        zkServer.stop();
//        zkServer.close();
//    }
//
//    public static void main(String[] args) throws Exception {
//        QndKafkaConsumerManual test = new QndKafkaConsumerManual();
//
//        test.qndAsyncProducer();
//        test.qndSyncNoAckProducer();
//        test.qndSyncLeaderAckProducer();
//        test.qndSyncAllAcksProducer();
//    }
//}
