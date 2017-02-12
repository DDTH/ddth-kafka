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
        final String GROUP_ID = "mygroupid";
        // final boolean CONSUME_FROM_BEGINNING = true;
        final KafkaClient.ProducerType PRODUCER_TYPE = KafkaClient.ProducerType.LEADER_ACK;
        final String TOPIC = "ddth-kafka";

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
                    }
                }
            };
            t.setDaemon(true);
            t.start();

            for (int i = 0; i < 10; i++) {
                KafkaMessage msg = new KafkaMessage(TOPIC, i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(PRODUCER_TYPE, msg);
                Thread.sleep(RAND.nextInt(1000) + 1);
            }

            Thread.sleep(4000);
        }
    }
}
