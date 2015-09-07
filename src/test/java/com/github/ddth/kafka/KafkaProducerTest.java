package com.github.ddth.kafka;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;
import junit.framework.TestSuite;

public class KafkaProducerTest extends BaseKafkaTest {

    public KafkaProducerTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(KafkaProducerTest.class);
    }

    private final static String TOPIC = "topic_test";
    private final String CONSUMER_GROUP_ID = "my-group-id";

    @org.junit.Test
    public void testNewInstance() throws Exception {
        assertNotNull(kafkaClient);
    }

    @org.junit.Test
    public void testConsumeOneByOne() throws Exception {
        createTopic(TOPIC);
        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
                "message - 6", "message - 7", "message - 8", "message - 9"));
        System.out.println("testConsumeOneByOne()====================");
        for (String content : TEST_MSGS) {
            KafkaMessage msg = new KafkaMessage(TOPIC, content, content);
            Future<KafkaMessage> resultMgs = kafkaClient.sendMessage(msg);
            msg = resultMgs.get(1000, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            System.out.println(content + "\t" + msg.partition() + " / " + msg.offset());
        }

        for (int i = 0, n = TEST_MSGS.size(); i < n; i++) {
            KafkaMessage msg = kafkaClient.consumeMessage(CONSUMER_GROUP_ID, true, TOPIC, 5000,
                    TimeUnit.MILLISECONDS);
            String strMsg = msg.contentAsString();
            assertTrue(TEST_MSGS.contains(strMsg));
            TEST_MSGS.remove(strMsg);
        }
    }

    @org.junit.Test
    public void testConsumeListenerAfter() throws Exception {
        createTopic(TOPIC);

        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
                "message - 6", "message - 7", "message - 8", "message - 9"));
        System.out.println("testConsumeListenerAfter()====================");
        for (String content : TEST_MSGS) {
            KafkaMessage msg = new KafkaMessage(TOPIC, content);
            Future<KafkaMessage> resultMgs = kafkaClient.sendMessage(msg);
            msg = resultMgs.get(1000, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            System.out.println(content + "\t" + msg.partition() + " / " + msg.offset());
        }

        final Set<String> RECEIVED_MSG = new HashSet<String>();
        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
            @Override
            public void onMessage(KafkaMessage message) {
                String msgStr = message != null ? message.contentAsString() : null;
                RECEIVED_MSG.add(msgStr);
            }
        };
        kafkaClient.addMessageListener(CONSUMER_GROUP_ID, true, TOPIC, msgListener);
        Thread.sleep(5000);

        assertEquals(TEST_MSGS.size(), RECEIVED_MSG.size());
        assertEquals(TEST_MSGS, RECEIVED_MSG);
    }

    @org.junit.Test
    public void testConsumeListenerBefore() throws Exception {
        createTopic(TOPIC);

        final Set<String> RECEIVED_MSG = new HashSet<String>();
        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
            @Override
            public void onMessage(KafkaMessage message) {
                String msgStr = message != null ? message.contentAsString() : null;
                RECEIVED_MSG.add(msgStr);
            }
        };
        kafkaClient.addMessageListener(CONSUMER_GROUP_ID, true, TOPIC, msgListener);

        final Set<String> TEST_MSGS = new HashSet<String>(Arrays.asList("message - 0",
                "message - 1", "message - 2", "message - 3", "message - 4", "message - 5",
                "message - 6", "message - 7", "message - 8", "message - 9"));
        System.out.println("testConsumeListenerBefore()====================");
        for (String content : TEST_MSGS) {
            KafkaMessage msg = new KafkaMessage(TOPIC, content);
            Future<KafkaMessage> resultMgs = kafkaClient.sendMessage(msg);
            msg = resultMgs.get(1000, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            System.out.println(content + "\t" + msg.partition() + " / " + msg.offset());
        }

        Thread.sleep(2000);

        assertEquals(TEST_MSGS.size(), RECEIVED_MSG.size());
        assertEquals(TEST_MSGS, RECEIVED_MSG);
    }
}
