package com.github.ddth.kafka.qnd;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

/*
 * mvn package exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.github.ddth.kafka.qnd.QndTestConsumeListener" -Dzookeeper=localhost:2181/kafka -Dtopic=topic -Dgroup=group-id-1 -DnumItems=100
 */
public class QndTestConsumeListener {

    final static Random RANDOM = new Random(System.currentTimeMillis());

    private static void queueAndConsume(KafkaClient kafkaClient, String topic,
            String consumerGroupId, int numItems) throws InterruptedException {

        System.out.println("Putting [" + numItems + "] items to queue [" + topic + " / "
                + consumerGroupId + "]...");
        for (int i = 0; i < numItems; i++) {
            String content = System.currentTimeMillis() + " - " + i;
            KafkaMessage kmsg = new KafkaMessage(topic, content);
            kafkaClient.sendMessage(kmsg);
        }

        Thread.sleep(1000);
        final BlockingQueue<KafkaMessage> queue = new LinkedBlockingQueue<KafkaMessage>();
        kafkaClient.addMessageListener(consumerGroupId, true, topic, new IKafkaMessageListener() {
            @Override
            public void onMessage(KafkaMessage message) {
                queue.add(message);
                // try {
                // Thread.sleep(RANDOM.nextInt(10));
                // } catch (InterruptedException e) {
                // }
            }
        });

        Thread.sleep(1000);
        int counter = 0;
        KafkaMessage kmsg = queue.poll(1000, TimeUnit.MILLISECONDS);
        while (kmsg != null) {
            counter++;
            kmsg = queue.poll(1000, TimeUnit.MILLISECONDS);
            if (kmsg == null) {
                kmsg = queue.poll(1000, TimeUnit.MILLISECONDS);
                if (kmsg == null) {
                    kmsg = queue.poll(1000, TimeUnit.MILLISECONDS);
                }
            }
        }
        System.out.println("Consumed [" + counter + "] items from queue [" + topic + " / "
                + consumerGroupId + "].");
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
        int numItems = 100;
        try {
            numItems = Integer.parseInt(System.getProperty("numItems", "100"));
        } catch (Exception e) {
            numItems = 100;
        }

        System.out.println("Zookeeper: " + zkConnString);
        System.out.println("Topic    : " + topic);
        System.out.println("Group    : " + consumerGroupId);
        System.out.println("Num Items: " + numItems);

        final KafkaClient kafkaClient = new KafkaClient(zkConnString);
        try {
            kafkaClient.init();

            queueAndConsume(kafkaClient, topic, consumerGroupId, numItems);
        } finally {
            kafkaClient.destroy();
        }

        Thread.sleep(4000);
    }

}
