package com.github.ddth.kafka.qnd;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QndTestBlockingQueue {
    public static void main(String[] args) throws Exception {
        final int NUM_ITEMS = 10;
        final BlockingQueue<String> QUEUE = new LinkedBlockingQueue<String>();

        Thread T = new Thread() {
            public void run() {
                final Random rand = new Random(System.currentTimeMillis());
                for (int i = 0; i < NUM_ITEMS; i++) {
                    QUEUE.add(String.valueOf(i));
                    try {
                        Thread.sleep(rand.nextInt(5000));
                    } catch (Exception e) {
                    }
                }
            }
        };
        T.start();

        for (int i = 0; i < NUM_ITEMS; i++) {
            String msg = QUEUE.take();
            System.out.println(i + ": " + msg);
        }
    }
}
