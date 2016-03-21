//package com.github.ddth.kafka.qnd;
//
//import java.util.Random;
//
//import kafka.server.KafkaServerStartable;
//
//import org.apache.curator.test.TestingServer;
//
//import com.github.ddth.kafka.IKafkaMessageListener;
//import com.github.ddth.kafka.KafkaClient;
//import com.github.ddth.kafka.KafkaMessage;
//
//public class QndKafkaConsumerMessageListener extends BaseQndKafka {
//
//    public void qndMessageListener() throws Exception {
//        Random rand = new Random(System.currentTimeMillis());
//        int timestamp = (int) (System.currentTimeMillis() / 1000);
//
//        TestingServer zkServer = newZkServer();
//        KafkaServerStartable kafkaServer = newKafkaServer(zkServer);
//
//        KafkaClient kafkaClient = newKafkaClient(zkServer);
//        final String consumerGroupId = "my-group-id";
//
//        // create topic
//        final String topic = "topic_test_" + rand.nextInt(timestamp);
//        createTopic(zkServer, topic);
//
//        kafkaClient.addMessageListener(consumerGroupId, true, topic, new IKafkaMessageListener() {
//            @Override
//            public void onMessage(KafkaMessage message) {
//                System.out.println(message != null ? message.contentAsString() : null);
//            }
//        });
//        Thread.sleep(2000);
//        for (int i = 0; i < 10; i++) {
//            KafkaMessage msg = new KafkaMessage(topic, "message - " + i + ": "
//                    + System.currentTimeMillis());
//            kafkaClient.sendMessage(msg);
//        }
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
//        QndKafkaConsumerMessageListener test = new QndKafkaConsumerMessageListener();
//        test.qndMessageListener();
//    }
//}
