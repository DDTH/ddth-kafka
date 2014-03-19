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

    @org.junit.Test
    public void testNewInstance() {
        KafkaProducer producer = new KafkaProducer(getKafkaBrokerString(),
                KafkaProducer.Type.SYNC_NO_ACK);
        try {
            assertNotNull(producer);
            producer.init();
        } finally {
            producer.destroy();
        }
    }

    @org.junit.Test
    public void testConsumeOneByOne() throws InterruptedException {
        createTopic(TOPIC);

        final String[] TEST_MSGS = new String[] { "message - 1", "message - 2", "message - 3" };
        for (String msg : TEST_MSGS) {
            kafkaProducer.sendMessage(TOPIC, msg);
        }

        for (int i = 0; i < TEST_MSGS.length; i++) {
            byte[] message = kafkaConsumer.consume(TOPIC, 5000, TimeUnit.MILLISECONDS);
            assertEquals(TEST_MSGS[i], new String(message));
        }
    }

    @org.junit.Test
    public void testConsumeListenerAfter() throws InterruptedException {
        createTopic(TOPIC);

        final String[] TEST_MSGS = new String[] { "message - 1", "message - 2", "message - 3" };
        for (String msg : TEST_MSGS) {
            kafkaProducer.sendMessage(TOPIC, msg);
        }

        final List<String> RECEIVED_MSG = new ArrayList<String>();
        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
            @Override
            public void onMessage(String topic, int partition, long offset, byte[] key,
                    byte[] message) {
                String msgStr = message != null ? new String(message) : null;
                RECEIVED_MSG.add(msgStr);
            }
        };
        kafkaConsumer.addMessageListener(TOPIC, msgListener);
        Thread.sleep(5000);

        assertEquals(TEST_MSGS.length, RECEIVED_MSG.size());
        for (int i = 0; i < TEST_MSGS.length; i++) {
            assertEquals(TEST_MSGS[i], RECEIVED_MSG.get(i));
        }
    }

    @org.junit.Test
    public void testConsumeListenerBefore() throws InterruptedException {
        createTopic(TOPIC);

        final List<String> RECEIVED_MSG = new ArrayList<String>();
        IKafkaMessageListener msgListener = new IKafkaMessageListener() {
            @Override
            public void onMessage(String topic, int partition, long offset, byte[] key,
                    byte[] message) {
                String msgStr = message != null ? new String(message) : null;
                RECEIVED_MSG.add(msgStr);
            }
        };
        kafkaConsumer.addMessageListener(TOPIC, msgListener);

        final String[] TEST_MSGS = new String[] { "message - 1", "message - 2", "message - 3" };
        for (String msg : TEST_MSGS) {
            kafkaProducer.sendMessage(TOPIC, msg);
        }

        Thread.sleep(2000);

        assertEquals(TEST_MSGS.length, RECEIVED_MSG.size());
        for (int i = 0; i < TEST_MSGS.length; i++) {
            assertEquals(TEST_MSGS[i], RECEIVED_MSG.get(i));
        }
    }
}
