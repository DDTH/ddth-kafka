package com.github.ddth.kafka.qnd;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerMessageListener {

    public void qndMessageListener() throws Exception {
        Random rand = new Random(System.currentTimeMillis());
        int NUM_MSGS = 10;
        int timestamp = (int) (System.currentTimeMillis() / 1000);

        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        // final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;
        // final KafkaClient.ProducerType PRODUCER_TYPE =
        // KafkaClient.ProducerType.FULL_ASYNC;

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();

            final String TOPIC = "topic_test_" + rand.nextInt(timestamp);
            // final String TOPIC = "demo";

            System.out.println(TOPIC);

            final AtomicLong COUNTER = new AtomicLong(0);
            kafkaClient.addMessageListener(GROUP_ID, CONSUME_FROM_BEGINNING, TOPIC,
                    new IKafkaMessageListener() {
                        @Override
                        public void onMessage(KafkaMessage message) {
                            COUNTER.incrementAndGet();
                            System.out.println(message != null ? message.contentAsString() : null);
                        }
                    });

            Thread.sleep(2000);
            for (int i = 0; i < NUM_MSGS; i++) {
                KafkaMessage msg = new KafkaMessage(TOPIC, "message - " + i + ": "
                        + System.currentTimeMillis());
                kafkaClient.sendMessage(msg).get();
                // System.out.println(kafkaClient.consumeMessage(GROUP_ID,
                // TOPIC, 1000,
                // TimeUnit.MILLISECONDS));
            }
            Thread.sleep(5000);

            System.out.println(COUNTER.get());
        }
    }

    public static void main(String[] args) throws Exception {
        QndConsumerMessageListener test = new QndConsumerMessageListener();
        test.qndMessageListener();
    }
}
