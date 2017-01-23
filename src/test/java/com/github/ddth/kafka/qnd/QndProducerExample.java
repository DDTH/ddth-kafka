package com.github.ddth.kafka.qnd;

import java.util.concurrent.Future;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndProducerExample {

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = "localhost:9092";
        final String topic = "demo1";

        try (KafkaClient kafkaClient = new KafkaClient(bootstrapServers)) {
            kafkaClient.init();

            for (int i = 0; i < 10; i++) {
                String msg = i + ":" + System.currentTimeMillis();
                byte[] content = msg.getBytes();
                Future<KafkaMessage> result = kafkaClient
                        .sendMessage(new KafkaMessage().topic(topic).content(content));
                // System.out.println(result.get(1000, TimeUnit.MILLISECONDS));
                System.out.println(result.get());
            }
        }

    }
}