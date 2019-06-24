package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class QndSameConsumerGroupBulk {

    private static AtomicLong NUM_SENT = new AtomicLong(0);
    private static AtomicLong NUM_TAKEN = new AtomicLong(0);
    private static AtomicLong NUM_EXCEPTION = new AtomicLong(0);
    private static ConcurrentMap<Object, Object> SENT;

    static {
        SENT = new ConcurrentHashMap<>();
    }

    private static ConcurrentMap<Object, Object> RECEIVE = new ConcurrentHashMap<>();
    private static AtomicLong TIMESTAMP = new AtomicLong(0);
    private static long NUM_ITEMS = 64 * 1024;
    private static int NUM_THREADS = 4;

    private static void emptyQueue(KafkaClient kafkaClient, String topic, String consumerGroupId) {
        System.out.print("Emptying queue...[" + topic + " / " + consumerGroupId + "]...");
        long t1 = System.currentTimeMillis();
        long counter = 0;
        KafkaMessage kmsg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000, TimeUnit.MILLISECONDS);
        while (kmsg != null) {
            counter++;
            kmsg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000, TimeUnit.MILLISECONDS);
        }
        System.out.println("Emptying queue..." + counter + " in " + (System.currentTimeMillis() - t1) + "ms.");
    }

    private static void putToQueue(KafkaClient kafkaClient, String topic, long numItems) {
        List<KafkaMessage> buffer = new ArrayList<>();
        for (int i = 0; i < numItems; i++) {
            String content = "Content: [" + i + "] " + new Date();
            KafkaMessage kmsg = new KafkaMessage(topic, content);
            buffer.add(kmsg);
            if (i % 100 == 0) {
                kafkaClient.sendBulk(buffer.toArray(KafkaMessage.EMPTY_ARRAY));
                buffer.clear();
            }
            NUM_SENT.incrementAndGet();
            SENT.put(content, Boolean.TRUE);
        }
        if (buffer.size() > 0) {
            kafkaClient.sendBulk(buffer.toArray(KafkaMessage.EMPTY_ARRAY));
            buffer.clear();
        }
    }

    /**
     * Run this class twice or three times.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final String brokers = "localhost:9092";
        final String topic = "t1partition";
        final String consumerGroupId = "mygroupid";

        try (KafkaClient kafkaClient = new KafkaClient(brokers)) {
            kafkaClient.init();
            emptyQueue(kafkaClient, topic, consumerGroupId);

            for (int i = 0; i < NUM_THREADS; i++) {
                Thread t = new Thread(() -> {
                    while (true) {
                        try {
                            KafkaMessage kmsg = kafkaClient
                                    .consumeMessage(consumerGroupId, false, topic, 1000, TimeUnit.MILLISECONDS);
                            if (kmsg != null) {
                                long numItems = NUM_TAKEN.incrementAndGet();
                                if (numItems >= NUM_ITEMS) {
                                    TIMESTAMP.set(System.currentTimeMillis());
                                }
                                RECEIVE.put(kmsg.contentAsString(), Boolean.TRUE);
                            } else {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                }
                            }
                        } catch (Exception e) {
                            NUM_EXCEPTION.incrementAndGet();
                            //e.printStackTrace();
                        }
                    }
                });
                t.setDaemon(true);
                t.start();
            }

            Thread.sleep(2000);

            long t1 = System.currentTimeMillis();
            putToQueue(kafkaClient, topic, NUM_ITEMS);
            long t2 = System.currentTimeMillis();

            long t = System.currentTimeMillis();
            while (NUM_TAKEN.get() < NUM_ITEMS && t - t2 < 60000) {
                Thread.sleep(1);
                t = System.currentTimeMillis();
            }
            System.out.println("Duration Queue: " + (t2 - t1));
            System.out.println("Duration Take : " + (TIMESTAMP.get() - t1));
            System.out.println("Num sent     : " + NUM_SENT.get());
            System.out.println("Num taken    : " + NUM_TAKEN.get());
            System.out.println("Num exception: " + NUM_EXCEPTION.get());
            System.out.println("Sent size    : " + SENT.size());
            System.out.println("Receive size : " + RECEIVE.size());
            System.out.println("Check        : " + SENT.equals(RECEIVE));

            Thread.sleep(1000);
        }
    }
}
