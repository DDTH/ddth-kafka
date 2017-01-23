package com.github.ddth.kafka.qnd;

import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

/*
 * mvn package exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.github.ddth.kafka.qnd.QndTestClearQueue" -Dbrokers=localhost:9092 -Dtopic=topic -Dgroup=group-id-1
 */
public class QndTestClearQueue {

    private static void emptyQueue(KafkaClient kafkaClient, String topic, String consumerGroupId)
            throws InterruptedException {
        System.out.print("Emptying queue...[" + topic + " / " + consumerGroupId + "]...");
        long t1 = System.currentTimeMillis();
        long counter = 0;
        KafkaMessage kmsg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                TimeUnit.MILLISECONDS);
        while (kmsg != null) {
            counter++;
            kmsg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                    TimeUnit.MILLISECONDS);
        }
        System.out.println(
                "Emptying queue..." + counter + " in " + (System.currentTimeMillis() - t1) + "ms.");
    }

    /**
     * Run this class twice or three times.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String brokers = System.getProperty("brokers", "localhost:9092");
        String topic = System.getProperty("topic", "demo");
        String consumerGroupId = System.getProperty("group", "mygroupid");

        System.out.println("Brokers  : " + brokers);
        System.out.println("Topic    : " + topic);
        System.out.println("Group    : " + consumerGroupId);

        final KafkaClient kafkaClient = new KafkaClient(brokers);
        try {
            kafkaClient.init();
            emptyQueue(kafkaClient, topic, consumerGroupId);
        } finally {
            kafkaClient.destroy();
        }

        Thread.sleep(1000);
    }

}
