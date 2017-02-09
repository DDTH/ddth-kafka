package com.github.ddth.kafka.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaMessage;

public class KafkaMsgConsumerWorker extends Thread {

    // private Logger LOGGER =
    // LoggerFactory.getLogger(KafkaMsgConsumerWorker.class);

    private KafkaMsgConsumer consumer;
    private Collection<IKafkaMessageListener> messageListerners;
    private String topic;
    private boolean stop = false;

    private ExecutorService executorService;

    public KafkaMsgConsumerWorker(KafkaMsgConsumer consumer, String topic,
            Collection<IKafkaMessageListener> messageListerners, ExecutorService executorService) {
        super(KafkaMsgConsumerWorker.class.getCanonicalName() + " - " + topic);
        this.consumer = consumer;
        this.topic = topic;
        this.messageListerners = messageListerners;
        this.executorService = executorService;
    }

    /**
     * Stops the worker.
     */
    public void stopWorker() {
        stop = true;
    }

    private void deliverMessage(KafkaMessage msg, Collection<IKafkaMessageListener> msgListeners) {
        AtomicInteger counter = new AtomicInteger(msgListeners.size());
        for (final IKafkaMessageListener listerner : msgListeners) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        listerner.onMessage(msg);
                    } finally {
                        counter.decrementAndGet();
                    }
                }
            });
        }
        while (counter.get() > 0)
            ;

        // if (true)
        // return;
        // final CountDownLatch countDownLatch = new
        // CountDownLatch(msgListeners.size());
        // for (final IKafkaMessageListener listerner : msgListeners) {
        // // Delivery the consumed message to n-listeners
        // // asynchronously
        // Thread t = new Thread("Kafka-Consumer-Delivery") {
        // public void run() {
        // try {
        // listerner.onMessage(msg);
        // } catch (Exception e) {
        // LOGGER.warn(e.getMessage(), e);
        // } finally {
        // countDownLatch.countDown();
        // }
        // }
        // };
        // t.start();
        // try {
        // countDownLatch.await();
        // } catch (InterruptedException e) {
        // LOGGER.warn(e.getMessage(), e);
        // }
        // }
    }

    @Override
    public void run() {
        Collection<IKafkaMessageListener> msgListeners = new HashSet<IKafkaMessageListener>();
        while (!stop) {
            msgListeners.clear();
            msgListeners.addAll(messageListerners);

            if (msgListeners.size() > 0) {
                KafkaMessage msg = consumer.consume(topic, 100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    deliverMessage(msg, msgListeners);
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }
    }
}
