package com.github.ddth.kafka.qnd;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndSameConsumerGroup {

    private static AtomicLong NUM_SENT = new AtomicLong(0);
    private static AtomicLong NUM_TAKEN = new AtomicLong(0);
    private static AtomicLong NUM_EXCEPTION = new AtomicLong(0);
    private static ConcurrentMap<Object, Object> SENT = new ConcurrentHashMap<Object, Object>();
    private static ConcurrentMap<Object, Object> RECEIVE = new ConcurrentHashMap<Object, Object>();
    private static AtomicLong TIMESTAMP = new AtomicLong(0);
    private static long NUM_ITEMS = 10240;
    private static int NUM_THREADS = 4;

    private static void emptyQueue(KafkaClient kafkaClient, String topic, String consumerGroupId)
            throws InterruptedException {
        System.out.println("Emptying queue...[" + topic + " / " + consumerGroupId + "]");
        long counter = 0;
        KafkaMessage kmsg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                TimeUnit.MILLISECONDS);
        while (kmsg != null) {
            kmsg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                    TimeUnit.MILLISECONDS);
            counter++;
        }
        System.out.println("Emptying queue..." + counter);
    }

    private static void putToQueue(KafkaClient kafkaClient, String topic, long numItems) {
        for (int i = 0; i < numItems; i++) {
            String content = "Content: [" + i + "] " + new Date();
            KafkaMessage kmsg = new KafkaMessage(topic, content);
            kafkaClient.sendMessage(kmsg);
            NUM_SENT.incrementAndGet();
            SENT.put(content, Boolean.TRUE);
        }
    }

    /**
     * Run this class twice or three times.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final String zkConnString = "localhost:2181/kafka";
        final String topic = "t4partition";
        final String consumerGroupId = "group-id-1";

        final KafkaClient kafkaClient = new KafkaClient(zkConnString);
        try {
            kafkaClient.init();
            emptyQueue(kafkaClient, topic, consumerGroupId);

            for (int i = 0; i < NUM_THREADS; i++) {
                Thread t = new Thread() {
                    public void run() {
                        while (true) {
                            try {
                                KafkaMessage kmsg = kafkaClient.consumeMessage(consumerGroupId,
                                        false, topic, 1000, TimeUnit.MILLISECONDS);
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
                                e.printStackTrace();
                            }
                        }
                    }
                };
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

            Thread.sleep(4000);
        } finally {
            kafkaClient.destroy();
        }
    }

}
