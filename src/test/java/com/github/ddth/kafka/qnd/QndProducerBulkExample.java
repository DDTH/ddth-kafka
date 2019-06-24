package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

import java.util.ArrayList;
import java.util.List;

public class QndProducerBulkExample {

    public static void main(String[] args) {
        final String bootstrapServers = "localhost:9092";
        final String topic = "t4partition";
        final int numMsgs = 64 * 1024;

        long t1 = System.currentTimeMillis();
        try (KafkaClient kafkaClient = new KafkaClient(bootstrapServers)) {
            kafkaClient.init();
            List<KafkaMessage> buffer = new ArrayList<>();
            for (int i = 1, n = numMsgs + 1; i < n; i++) {
                String msg = i + ":" + System.nanoTime();
                byte[] content = msg.getBytes();
                buffer.add(new KafkaMessage().topic(topic).content(content));
                if (i % 100 == 0) {
                    kafkaClient.sendBulk(buffer.toArray(KafkaMessage.EMPTY_ARRAY));
                    buffer.clear();
                }
            }
            if (buffer.size() > 0) {
                kafkaClient.sendBulk(buffer.toArray(KafkaMessage.EMPTY_ARRAY));
                buffer.clear();
            }
        }
        long d = System.currentTimeMillis() - t1;
        System.out.println(
                "Sent " + numMsgs + " messages in " + d + "ms (" + String.format("%,.1f", numMsgs * 1000.0 / d)
                        + " msgs/s)");
    }
}
