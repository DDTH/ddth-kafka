package com.github.ddth.kafka.qnd;

import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerExample2 {

    public static void main(String[] args) throws Exception {
        final String zkConnString = "localhost:2181/kafka";
        final String topic = "t4partition";
        final String consumerGroupId = "group-id-2";

        KafkaClient kafkaClient = new KafkaClient(zkConnString);
        try {
            kafkaClient.init();

            final int NUM_ITEMS = 1000;
            for (int i = 0; i < NUM_ITEMS; i++) {
                String content = i + ":" + System.currentTimeMillis();
                KafkaMessage msg = new KafkaMessage(topic, content);
                kafkaClient.sendMessage(msg);
            }
            System.out.println("Num sent: " + NUM_ITEMS);

            int COUNTER = 0;
            KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                    TimeUnit.MILLISECONDS);
            while (msg != null) {
                COUNTER++;
                msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                        TimeUnit.MILLISECONDS);
            }
            System.out.println("Num received: " + COUNTER);
        } finally {
            kafkaClient.destroy();
        }
    }
}
