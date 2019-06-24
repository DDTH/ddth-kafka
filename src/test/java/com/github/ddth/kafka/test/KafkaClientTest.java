//package com.github.ddth.kafka.test;
//
//import java.util.Arrays;
//import java.util.HashSet;
//import java.util.Set;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//
//import com.github.ddth.kafka.IKafkaMessageListener;
//import com.github.ddth.kafka.KafkaMessage;
//
//import junit.framework.Test;
//import junit.framework.TestSuite;
//
//public class KafkaClientTest extends BaseKafkaTest {
//
//    public KafkaClientTest(String testName) {
//        super(testName);
//    }
//
//    public static Test suite() {
//        return new TestSuite(KafkaClientTest.class);
//    }
//
//    private final static String TOPIC = "test";
//    private final String CONSUMER_GROUP_ID = "mygroupid";
//
//    @org.junit.Test
//    public void testNewInstance() throws Exception {
//        assertNotNull(kafkaClient);
//    }
//
//    @org.junit.Test
//    public void testSendSyncConsumeOneByOne() throws Exception {
//        createTopic(TOPIC);
//        warnup(TOPIC, CONSUMER_GROUP_ID);
//
//        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
//                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
//                "message - 6", "message - 7", "message - 8", "message - 9"));
//        for (String content : TEST_MSGS) {
//            KafkaMessage msg = new KafkaMessage(TOPIC, content, content);
//            KafkaMessage result = kafkaClient.sendMessage(msg);
//            assertNotNull(result);
//        }
//
//        for (int i = 0, n = TEST_MSGS.size(); i < n; i++) {
//            KafkaMessage msg = kafkaClient.consumeMessage(CONSUMER_GROUP_ID, true, TOPIC, 5000,
//                    TimeUnit.MILLISECONDS);
//            String strMsg = msg.contentAsString();
//            assertTrue(TEST_MSGS.contains(strMsg));
//            TEST_MSGS.remove(strMsg);
//        }
//    }
//
//    @org.junit.Test
//    public void testSendAsyncConsumeOneByOne() throws Exception {
//        createTopic(TOPIC);
//        warnup(TOPIC, CONSUMER_GROUP_ID);
//
//        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
//                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
//                "message - 6", "message - 7", "message - 8", "message - 9"));
//        for (String content : TEST_MSGS) {
//            KafkaMessage msg = new KafkaMessage(TOPIC, content, content);
//            Future<KafkaMessage> result = kafkaClient.sendMessageAsync(msg);
//            assertNotNull(result);
//        }
//
//        for (int i = 0, n = TEST_MSGS.size(); i < n; i++) {
//            KafkaMessage msg = kafkaClient.consumeMessage(CONSUMER_GROUP_ID, true, TOPIC, 10000,
//                    TimeUnit.MILLISECONDS);
//            String strMsg = msg.contentAsString();
//            assertTrue(TEST_MSGS.contains(strMsg));
//            TEST_MSGS.remove(strMsg);
//        }
//    }
//
//    @org.junit.Test
//    public void testSendSyncConsumeListenerAfter() throws Exception {
//        createTopic(TOPIC);
//        warnup(TOPIC, CONSUMER_GROUP_ID);
//
//        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
//                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
//                "message - 6", "message - 7", "message - 8", "message - 9"));
//        for (String content : TEST_MSGS) {
//            KafkaMessage msg = new KafkaMessage(TOPIC, content);
//            KafkaMessage result = kafkaClient.sendMessage(msg);
//            assertNotNull(result);
//        }
//
//        final Set<String> RECEIVED_MSG = new HashSet<String>();
//        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
//            @Override
//            public void onMessage(KafkaMessage message) {
//                String msgStr = message != null ? message.contentAsString() : null;
//                RECEIVED_MSG.add(msgStr);
//            }
//        };
//        kafkaClient.addMessageListener(CONSUMER_GROUP_ID, true, TOPIC, msgListener);
//        long t = System.currentTimeMillis();
//        while (RECEIVED_MSG.size() < TEST_MSGS.size() && System.currentTimeMillis() - t < 30000) {
//            Thread.sleep(1);
//        }
//
//        assertEquals(TEST_MSGS.size(), RECEIVED_MSG.size());
//        assertEquals(TEST_MSGS, RECEIVED_MSG);
//    }
//
//    @org.junit.Test
//    public void testSendAsyncConsumeListenerAfter() throws Exception {
//        createTopic(TOPIC);
//        warnup(TOPIC, CONSUMER_GROUP_ID);
//
//        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
//                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
//                "message - 6", "message - 7", "message - 8", "message - 9"));
//        for (String content : TEST_MSGS) {
//            KafkaMessage msg = new KafkaMessage(TOPIC, content);
//            Future<KafkaMessage> result = kafkaClient.sendMessageAsync(msg);
//            assertNotNull(result);
//        }
//
//        final Set<String> RECEIVED_MSG = new HashSet<String>();
//        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
//            @Override
//            public void onMessage(KafkaMessage message) {
//                String msgStr = message != null ? message.contentAsString() : null;
//                RECEIVED_MSG.add(msgStr);
//            }
//        };
//        kafkaClient.addMessageListener(CONSUMER_GROUP_ID, true, TOPIC, msgListener);
//        long t = System.currentTimeMillis();
//        while (RECEIVED_MSG.size() < TEST_MSGS.size() && System.currentTimeMillis() - t < 30000) {
//            Thread.sleep(1);
//        }
//
//        assertEquals(TEST_MSGS.size(), RECEIVED_MSG.size());
//        assertEquals(TEST_MSGS, RECEIVED_MSG);
//    }
//
//    @org.junit.Test
//    public void testSendSyncConsumeListenerBefore() throws Exception {
//        createTopic(TOPIC);
//        warnup(TOPIC, CONSUMER_GROUP_ID);
//
//        final Set<String> RECEIVED_MSG = new HashSet<String>();
//        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
//            @Override
//            public void onMessage(KafkaMessage message) {
//                String msgStr = message != null ? message.contentAsString() : null;
//                RECEIVED_MSG.add(msgStr);
//            }
//        };
//        kafkaClient.addMessageListener(CONSUMER_GROUP_ID, true, TOPIC, msgListener);
//
//        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
//                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
//                "message - 6", "message - 7", "message - 8", "message - 9"));
//        for (String content : TEST_MSGS) {
//            KafkaMessage msg = new KafkaMessage(TOPIC, content);
//            KafkaMessage result = kafkaClient.sendMessage(msg);
//            assertNotNull(result);
//        }
//        long t = System.currentTimeMillis();
//        while (RECEIVED_MSG.size() < TEST_MSGS.size() && System.currentTimeMillis() - t < 30000) {
//            Thread.sleep(1);
//        }
//
//        assertEquals(TEST_MSGS.size(), RECEIVED_MSG.size());
//        assertEquals(TEST_MSGS, RECEIVED_MSG);
//    }
//
//    @org.junit.Test
//    public void testSendAsyncConsumeListenerBefore() throws Exception {
//        createTopic(TOPIC);
//        warnup(TOPIC, CONSUMER_GROUP_ID);
//
//        final Set<String> RECEIVED_MSG = new HashSet<String>();
//        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
//            @Override
//            public void onMessage(KafkaMessage message) {
//                String msgStr = message != null ? message.contentAsString() : null;
//                RECEIVED_MSG.add(msgStr);
//            }
//        };
//        kafkaClient.addMessageListener(CONSUMER_GROUP_ID, true, TOPIC, msgListener);
//
//        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
//                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
//                "message - 6", "message - 7", "message - 8", "message - 9"));
//        for (String content : TEST_MSGS) {
//            KafkaMessage msg = new KafkaMessage(TOPIC, content);
//            Future<KafkaMessage> result = kafkaClient.sendMessageAsync(msg);
//            assertNotNull(result);
//        }
//        long t = System.currentTimeMillis();
//        while (RECEIVED_MSG.size() < TEST_MSGS.size() && System.currentTimeMillis() - t < 30000) {
//            Thread.sleep(1);
//        }
//
//        assertEquals(TEST_MSGS.size(), RECEIVED_MSG.size());
//        assertEquals(TEST_MSGS, RECEIVED_MSG);
//    }
//}
