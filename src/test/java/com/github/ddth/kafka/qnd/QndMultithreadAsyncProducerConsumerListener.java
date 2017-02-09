package com.github.ddth.kafka.qnd;

import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndMultithreadAsyncProducerConsumerListener {

    private final static IdGenerator idGen = IdGenerator.getInstance(0);

    private static Thread[] createProducerThreads(KafkaClient kafkaClient, String topic,
            AtomicLong counterSent, int numThreads, final int numMsgs) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Producer - " + i) {
                public void run() {
                    Callback callback = new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                counterSent.incrementAndGet();
                            }
                        }
                    };
                    for (int i = 0; i < numMsgs; i++) {
                        String content = i + ":" + idGen.generateId128Hex();
                        KafkaMessage msg = new KafkaMessage(topic, content);
                        kafkaClient.sendMessageRaw(msg, callback);
                    }
                }
            };
        }
        return result;
    }

    public static void flush(KafkaClient kafkaClient, String topic, String groupId) {
        kafkaClient.seekToEnd(groupId, topic);

        int numMsgs = 0;
        long t1 = System.currentTimeMillis();
        KafkaMessage msg = kafkaClient.consumeMessage(groupId, topic);
        while (msg != null) {
            numMsgs++;
            msg = kafkaClient.consumeMessage(groupId, topic);
        }
        System.out.println(
                "* Flush " + numMsgs + " msgs in " + (System.currentTimeMillis() - t1) + "ms.");
    }

    public static void run(KafkaClient kafkaClient, String topic, String groupId, int numMsgs,
            int numProducers) throws InterruptedException {
        final AtomicLong COUNTER_SENT = new AtomicLong(0);
        final AtomicLong COUNTER_RECEIVED = new AtomicLong(0);

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(kafkaClient, topic, COUNTER_SENT, numProducers,
                numMsgs / numProducers);
        for (Thread th : producers) {
            th.start();
        }

        long t = System.currentTimeMillis();
        IKafkaMessageListener messageListener = new IKafkaMessageListener() {
            @Override
            public void onMessage(KafkaMessage message) {
                if (message != null) {
                    COUNTER_RECEIVED.incrementAndGet();
                } else {
                    System.out.println("Something wrong!");
                    throw new IllegalStateException();
                }
            }
        };
        kafkaClient.addMessageListener(groupId, true, topic, messageListener);
        for (Thread th : producers) {
            th.join();
        }
        while (COUNTER_RECEIVED.get() < numMsgs && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        long d = t - t1;

        kafkaClient.removeMessageListener(groupId, topic, messageListener);

        System.out.println(MessageFormat.format("== TEST - {0}P", numProducers));
        if (COUNTER_SENT.get() != COUNTER_RECEIVED.get()) {
            System.out.print("[F]");
        } else {
            System.out.print("[T]");
        }
        System.out.println("  Msgs: " + numMsgs + " - " + COUNTER_SENT.get() + " - "
                + COUNTER_RECEIVED.get() + " / Duration: " + d + "ms - "
                + String.format("%,.1f", numMsgs * 1000.0 / d) + " msg/s");
    }

    public static void main(String[] args) throws Exception {
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String TOPIC = "ddth-kafka";
        final String GROUP_ID = "ddth-kafka";
        final int NUM_MSGS = 8 * 1024;

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();
            flush(kafkaClient, TOPIC, GROUP_ID);
            run(kafkaClient, TOPIC, GROUP_ID, NUM_MSGS, 1);
        }

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();
            flush(kafkaClient, TOPIC, GROUP_ID);
            run(kafkaClient, TOPIC, GROUP_ID, NUM_MSGS, 2);
        }

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();
            flush(kafkaClient, TOPIC, GROUP_ID);
            run(kafkaClient, TOPIC, GROUP_ID, NUM_MSGS, 4);
        }

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();
            flush(kafkaClient, TOPIC, GROUP_ID);
            run(kafkaClient, TOPIC, GROUP_ID, NUM_MSGS, 8);
        }
    }

}
