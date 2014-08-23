package com.github.ddth.kafka;

import java.util.ArrayList;
import java.util.List;
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
        KafkaClient kafkaClient = new KafkaClient(getKafkaBrokerString());
        try {
            assertNotNull(kafkaClient);
            kafkaClient.init();
        } finally {
            kafkaClient.destroy();
        }
    }

    @org.junit.Test
    public void testConsumeOneByOne() throws Exception {
        createTopic(TOPIC);

        final String[] TEST_MSGS = new String[] { "message - 1", "message - 2", "message - 3" };
        for (String content : TEST_MSGS) {
            KafkaMessage msg = new KafkaMessage(TOPIC, content);
            kafkaClient.sendMessage(msg);
        }

        for (int i = 0; i < TEST_MSGS.length; i++) {
            KafkaMessage msg = kafkaClient.consumeMessage(CONSUMER_GROUP_ID, true, TOPIC, 5000,
                    TimeUnit.MILLISECONDS);
            assertEquals(TEST_MSGS[i], msg.contentAsString());
        }
    }

    @org.junit.Test
    public void testConsumeListenerAfter() throws Exception {
        createTopic(TOPIC);

        final String[] TEST_MSGS = new String[] { "message - 1", "message - 2", "message - 3" };
        for (String content : TEST_MSGS) {
            KafkaMessage msg = new KafkaMessage(TOPIC, content);
            kafkaClient.sendMessage(msg);
        }

        final List<String> RECEIVED_MSG = new ArrayList<String>();
        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
            @Override
            public void onMessage(KafkaMessage message) {
                String msgStr = message != null ? message.contentAsString() : null;
                RECEIVED_MSG.add(msgStr);
            }
        };
        kafkaClient.addMessageListener(CONSUMER_GROUP_ID, true, TOPIC, msgListener);
        Thread.sleep(5000);

        assertEquals(TEST_MSGS.length, RECEIVED_MSG.size());
        for (int i = 0; i < TEST_MSGS.length; i++) {
            assertEquals(TEST_MSGS[i], RECEIVED_MSG.get(i));
        }
    }

    @org.junit.Test
    public void testConsumeListenerBefore() throws Exception {
        createTopic(TOPIC);

        final List<String> RECEIVED_MSG = new ArrayList<String>();
        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
            @Override
            public void onMessage(KafkaMessage message) {
                String msgStr = message != null ? message.contentAsString() : null;
                RECEIVED_MSG.add(msgStr);
            }
        };
        kafkaClient.addMessageListener(CONSUMER_GROUP_ID, true, TOPIC, msgListener);

        final String[] TEST_MSGS = new String[] { "message - 1", "message - 2", "message - 3" };
        for (String content : TEST_MSGS) {
            KafkaMessage msg = new KafkaMessage(TOPIC, content);
            kafkaClient.sendMessage(msg);
        }

        Thread.sleep(2000);

        assertEquals(TEST_MSGS.length, RECEIVED_MSG.size());
        for (int i = 0; i < TEST_MSGS.length; i++) {
            assertEquals(TEST_MSGS[i], RECEIVED_MSG.get(i));
        }
    }
}
