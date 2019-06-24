package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndProducerSyncExample {

    public static void main(String[] args) {
        final String bootstrapServers = "localhost:9092";
        final String topic = "t4partition";
        final int numMsgs = 4 * 1024;

        long t1 = System.currentTimeMillis();
        try (KafkaClient kafkaClient = new KafkaClient(bootstrapServers)) {
            kafkaClient.init();
            for (int i = 1, n = numMsgs + 1; i < n; i++) {
                String msg = i + ":" + System.nanoTime();
                byte[] content = msg.getBytes();
                kafkaClient.sendMessage(new KafkaMessage().topic(topic).content(content));
            }
        }
        long d = System.currentTimeMillis() - t1;
        System.out.println(
                "Sent " + numMsgs + " messages in " + d + "ms (" + String.format("%,.1f", numMsgs * 1000.0 / d)
                        + " msgs/s)");
    }
}
