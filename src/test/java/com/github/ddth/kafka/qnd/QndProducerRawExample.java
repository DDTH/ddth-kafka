package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;
import org.apache.kafka.clients.producer.Callback;

import java.util.concurrent.atomic.AtomicInteger;

public class QndProducerRawExample {
    public static void main(String[] args) {
        final String bootstrapServers = "localhost:9092";
        final String topic = "t4partition";
        final int numMsgs = 256 * 1024;
        final AtomicInteger numOk = new AtomicInteger(0), numFailed = new AtomicInteger(0);

        long t1 = System.currentTimeMillis();
        try (KafkaClient kafkaClient = new KafkaClient(bootstrapServers)) {
            kafkaClient.init();
            Callback callback = (metadata, exception) -> {
                if (exception == null) {
                    numOk.incrementAndGet();
                } else {
                    numFailed.incrementAndGet();
                    System.out.println(exception.getMessage());
                }
            };
            for (int i = 1, n = numMsgs + 1; i < n; i++) {
                String msg = i + ":" + System.nanoTime();
                byte[] content = msg.getBytes();
                kafkaClient.sendMessageRaw(new KafkaMessage().topic(topic).content(content), callback);
            }
        }
        long d = System.currentTimeMillis() - t1;
        System.out.println(
                "Sent " + numMsgs + " messages in " + d + "ms (" + String.format("%,.1f", numMsgs * 1000.0 / d)
                        + " msgs/s)\tOk: " + numOk.get() + ", Failed: " + numFailed.get());
    }
}
