package com.github.ddth.kafka.internal;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A simple worker that constantly polls messages from Kafka and delivers to subscribers.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class KafkaMsgConsumerWorker extends Thread {
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaMsgConsumerWorker.class);

    private KafkaMsgConsumer consumer;
    private Collection<IKafkaMessageListener> subscribers = new HashSet<>();
    private String topic;
    private boolean stop = false;
    private ExecutorService executorService;

    public KafkaMsgConsumerWorker(KafkaMsgConsumer consumer, String topic,
            Collection<IKafkaMessageListener> subscribers, ExecutorService executorService) {
        super(KafkaMsgConsumerWorker.class.getCanonicalName() + " - " + topic);
        this.consumer = consumer;
        this.topic = topic;
        this.executorService = executorService;
        setSubscribers(subscribers);
    }

    /**
     * Stops the worker.
     */
    public void stopWorker() {
        stop = true;
    }

    private void deliverMessage(KafkaMessage msg, Collection<IKafkaMessageListener> msgListeners) {
        CountDownLatch counter = new CountDownLatch(msgListeners.size());
        for (final IKafkaMessageListener listerner : msgListeners) {
            executorService.submit(() -> {
                try {
                    listerner.onMessage(msg);
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                } finally {
                    counter.countDown();
                }
            });
        }
        try {
            if (!counter.await(30, TimeUnit.SECONDS)) {
                LOGGER.warn("Message has not delivered successfully to all subscribers within 30 seconds.");
            }
        } catch (InterruptedException e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    /**
     * Set/Update subscriber list.
     *
     * @param subscribers
     * @return
     * @since 2.0.0
     */
    public KafkaMsgConsumerWorker setSubscribers(Collection<IKafkaMessageListener> subscribers) {
        synchronized (this.subscribers) {
            this.subscribers.clear();
            if (subscribers != null) {
                this.subscribers.addAll(subscribers);
            }
        }
        return this;
    }

    @Override
    public void run() {
        Collection<IKafkaMessageListener> copiedSubscribers = new HashSet<>();
        while (!stop) {
            copiedSubscribers.clear();
            synchronized (subscribers) {
                copiedSubscribers.addAll(subscribers);
            }
            if (copiedSubscribers.size() > 0) {
                try {
                    KafkaMessage msg = consumer.consume(topic, 100, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        deliverMessage(msg, copiedSubscribers);
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            Thread.yield();
        }
    }
}
