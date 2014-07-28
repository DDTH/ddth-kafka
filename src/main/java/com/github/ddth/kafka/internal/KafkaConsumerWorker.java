package com.github.ddth.kafka.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.kafka.IKafkaMessageListener;

public class KafkaConsumerWorker implements Runnable {

    private Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerWorker.class);

    private KafkaStream<byte[], byte[]> kafkaStream;
    private boolean stop = false;
    private Collection<IKafkaMessageListener> messageListerners;

    public KafkaConsumerWorker(KafkaStream<byte[], byte[]> kafkaStream,
            Collection<IKafkaMessageListener> messageListerners) {
        this.kafkaStream = kafkaStream;
        this.messageListerners = messageListerners;
    }

    /**
     * Stops the worker.
     */
    public void stop() {
        stop = true;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        while (!stop) {
            Collection<IKafkaMessageListener> msgListeners = new HashSet<IKafkaMessageListener>();
            MessageAndMetadata<byte[], byte[]> mm = null;
            synchronized (messageListerners) {
                if (messageListerners.size() > 0 && it.hasNext()) {
                    msgListeners.addAll(messageListerners);
                    mm = it.next();
                }
            }
            if (msgListeners.size() > 0 && mm != null) {
                final String topic = mm.topic();
                final int partition = mm.partition();
                final long offset = mm.offset();
                final byte[] key = mm.key();
                final byte[] message = mm.message();
                final CountDownLatch countDownLatch = new CountDownLatch(msgListeners.size());
                for (final IKafkaMessageListener listerner : messageListerners) {
                    Thread t = new Thread("Kafka-Consumer-Delivery") {
                        public void run() {
                            try {
                                listerner.onMessage(topic, partition, offset, key, message);
                            } catch (Exception e) {
                                LOGGER.warn(e.getMessage(), e);
                            } finally {
                                countDownLatch.countDown();
                            }
                        }
                    };
                    t.start();
                }
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            } else {
                Thread.yield();
            }
        }
    }
}
