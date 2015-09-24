package com.github.ddth.kafka.qnd;

import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

/*
 * mvn package exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.github.ddth.kafka.qnd.QndTestClearQueue" -Dzookeeper=localhost:2181/kafka -Dtopic=topic -Dgroup=group-id-1
 */
public class QndTestClearQueue {

    private static void emptyQueue(KafkaClient kafkaClient, String topic, String consumerGroupId)
            throws InterruptedException {
        System.out.println("Emptying queue...[" + topic + " / " + consumerGroupId + "]");
        long counter = 0;
        KafkaMessage kmsg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                TimeUnit.MILLISECONDS);
        while (kmsg != null) {
            counter++;
            kmsg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                    TimeUnit.MILLISECONDS);
        }
        System.out.println("Emptying queue..." + counter);
    }

    /**
     * Run this class twice or three times.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String zkConnString = System.getProperty("zookeeper", "localhost:2181/kafka");
        String topic = System.getProperty("topic", "t4partition");
        String consumerGroupId = System.getProperty("group", "group-id-1");

        System.out.println("Zookeeper: " + zkConnString);
        System.out.println("Topic    : " + topic);
        System.out.println("Group    : " + consumerGroupId);

        final KafkaClient kafkaClient = new KafkaClient(zkConnString);
        try {
            kafkaClient.init();
            emptyQueue(kafkaClient, topic, consumerGroupId);
        } finally {
            kafkaClient.destroy();
        }

        Thread.sleep(4000);
    }

}
