package com.github.ddth.kafka.qnd;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerThread {

    public static void main(String[] args) throws Exception {
        final Random RAND = new Random(System.currentTimeMillis());
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        // final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;
        final KafkaClient.ProducerType PRODUCER_TYPE = KafkaClient.ProducerType.FULL_ASYNC;

        final String TOPIC = "demo";
        // final KafkaConsumer<String, byte[]> consumer =
        // KafkaHelper.createKafkaConsumer(
        // BOOTSTRAP_SERVERS, GROUP_ID, CONSUME_FROM_BEGINNING, true, true);
        // consumer.subscribe(Arrays.asList(TOPIC));

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();

            Thread t = new Thread() {
                public void run() {
                    while (true) {
                        KafkaMessage msg = kafkaClient.consumeMessage(GROUP_ID, TOPIC, 1000,
                                TimeUnit.MILLISECONDS);
                        if (msg != null) {
                            System.out.println(msg);
                        }
                        // ConsumerRecords<String, byte[]> crList = null;
                        // synchronized (consumer) {
                        // crList = consumer.poll(1000);
                        // }
                        // if (crList != null && crList.count() > 0) {
                        // System.out.println(crList);
                        // }
                    }
                }
            };
            t.start();

            for (int i = 0; i < 10; i++) {
                KafkaMessage msg = new KafkaMessage(TOPIC, i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(PRODUCER_TYPE, msg).get();
                // System.out.println("Sent: " +
                // kafkaClient.sendMessage(PRODUCER_TYPE, msg).get());
                Thread.sleep(1000);
                // ConsumerRecords<String, byte[]> crList = consumer.poll(1000);
                // if (crList != null && crList.count() > 0) {
                // System.out.println("Received: " + crList);
                // }
            }
        }
    }
}
