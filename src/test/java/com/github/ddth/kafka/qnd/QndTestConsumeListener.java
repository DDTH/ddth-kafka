package com.github.ddth.kafka.qnd;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

/*
 * mvn package exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.github.ddth.kafka.qnd.QndTestConsumeListener" -Dbrokers=localhost:9092 -Dtopic=topic -Dgroup=group-id-1 -DnumItems=100
 */
public class QndTestConsumeListener {

    final static Random RANDOM = new Random(System.currentTimeMillis());

    private static void queueAndConsume(KafkaClient kafkaClient, String topic,
            String consumerGroupId, int numItems) throws InterruptedException {

        System.out.print("Putting [" + numItems + "] items to queue [" + topic + " / "
                + consumerGroupId + "]...");
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < numItems; i++) {
            String content = System.currentTimeMillis() + " - " + i;
            KafkaMessage kmsg = new KafkaMessage(topic, content);
            kafkaClient.sendMessage(kmsg);
        }
        System.out.println((System.currentTimeMillis() - t1) + "ms");

        Thread.sleep(1000);

        t1 = System.currentTimeMillis();
        final BlockingQueue<KafkaMessage> queue = new LinkedBlockingQueue<KafkaMessage>();
        kafkaClient.addMessageListener(consumerGroupId, false, topic, new IKafkaMessageListener() {
            @Override
            public void onMessage(KafkaMessage message) {
                queue.add(message);
                // try {
                // Thread.sleep(RANDOM.nextInt(10));
                // } catch (InterruptedException e) {
                // }
            }
        });

        int counter = 0;
        KafkaMessage kmsg = queue.poll(1000, TimeUnit.MILLISECONDS);
        while (kmsg != null || (counter < numItems && System.currentTimeMillis() - t1 < 60000)) {
            if (kmsg != null) {
                counter++;
            }
            kmsg = queue.poll(1000, TimeUnit.MILLISECONDS);
        }
        System.out.println("Consumed [" + counter + "] items from queue [" + topic + " / "
                + consumerGroupId + "] in " + (System.currentTimeMillis() - t1) + "ms.");
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
        int numItems = 100;
        try {
            numItems = Integer.parseInt(System.getProperty("numItems", "100"));
        } catch (Exception e) {
            numItems = 100;
        }

        System.out.println("Brokers  : " + brokers);
        System.out.println("Topic    : " + topic);
        System.out.println("Group    : " + consumerGroupId);
        System.out.println("Num Items: " + numItems);

        try (KafkaClient kafkaClient = new KafkaClient(brokers)) {
            kafkaClient.init();
            queueAndConsume(kafkaClient, topic, consumerGroupId, numItems);
        }

        Thread.sleep(1000);
    }

}
