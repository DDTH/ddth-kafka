package com.github.ddth.kafka.internal;

import java.util.Collection;

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
            synchronized (messageListerners) {
                if (messageListerners.size() > 0 && it.hasNext()) {
                    MessageAndMetadata<byte[], byte[]> mm = it.next();
                    String topic = mm.topic();
                    int partition = mm.partition();
                    long offset = mm.offset();
                    byte[] key = mm.key();
                    byte[] message = mm.message();
                    for (IKafkaMessageListener listerner : messageListerners) {
                        try {
                            listerner.onMessage(topic, partition, offset, key, message);
                        } catch (Exception e) {
                            LOGGER.warn(e.getMessage(), e);
                        }
                    }
                }
            }
            Thread.yield();
        }
    }
}
