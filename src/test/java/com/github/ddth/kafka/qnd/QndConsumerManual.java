package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

import java.util.concurrent.TimeUnit;

public class QndConsumerManual {

    private final static int NUM_MSGS = 1000;
    private final static String TOPIC = "t1partition";
    private final static String GROUP_ID = "mygroupid";
    private final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static void flush(KafkaClient kafkaClient, String groupId, String topic) {
        kafkaClient.seekToEnd(groupId, groupId);

        KafkaMessage msg = kafkaClient.consumeMessage(groupId, true, topic, 10000, TimeUnit.MILLISECONDS);
        while (msg != null) {
            msg = kafkaClient.consumeMessage(groupId, true, topic, 1000, TimeUnit.MILLISECONDS);
        }
    }

    private static void qnd(int numMsgs, String bootstrapServers, KafkaClient.ProducerType producerType, String groupId,
            String topic, boolean consumeFromBeginning) throws Exception {
        try (KafkaClient kafkaClient = new KafkaClient(bootstrapServers)) {
            kafkaClient.init();
            flush(kafkaClient, groupId, topic);

            long timestart = System.currentTimeMillis();
            long receivedMsgs = 0;
            for (int i = 0; i < numMsgs; i++) {
                KafkaMessage msg = new KafkaMessage(topic, "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(producerType, msg);
                //long t1 = System.currentTimeMillis();
                msg = kafkaClient.consumeMessage(groupId, consumeFromBeginning, topic, 10000, TimeUnit.MILLISECONDS);
                //long t2 = System.currentTimeMillis();
                if (msg != null) {
                    receivedMsgs++;
                }
                //System.out.println((msg != null ? msg.contentAsString() : null) + "\t" + (t2 - t1));
            }
            long timeEnd = System.currentTimeMillis();
            long d = timeEnd - timestart;
            System.out.println("Total: " + receivedMsgs + " msgs in " + d + "ms / " + Math
                    .round(receivedMsgs * 1000.0 / (double) d) + " msg/sec");
            Thread.sleep(2000);
        }
    }

    public void qndNoAckProducer() throws Exception {
        System.out.println("========== QND: NoAck Producer ==========");
        qnd(NUM_MSGS, BOOTSTRAP_SERVERS, KafkaClient.ProducerType.NO_ACK, GROUP_ID, TOPIC, true);
    }

    public void qndLeaderAckProducer() throws Exception {
        System.out.println("========== QND: LeaderAck Producer ==========");
        qnd(NUM_MSGS, BOOTSTRAP_SERVERS, KafkaClient.ProducerType.LEADER_ACK, GROUP_ID, TOPIC, true);
    }

    public void qndAllAcksProducer() throws Exception {
        System.out.println("========== QND: AllAcks Producer ==========");
        qnd(NUM_MSGS, BOOTSTRAP_SERVERS, KafkaClient.ProducerType.ALL_ACKS, GROUP_ID, TOPIC, true);
    }

    public static void main(String[] args) throws Exception {
        QndConsumerManual test = new QndConsumerManual();

        test.qndNoAckProducer();
        test.qndLeaderAckProducer();
        test.qndAllAcksProducer();
    }
}
