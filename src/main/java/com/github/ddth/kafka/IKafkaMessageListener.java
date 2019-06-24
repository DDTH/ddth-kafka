package com.github.ddth.kafka;

/**
 * Kafka message listener interface.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public interface IKafkaMessageListener {
    /**
     * Called when a message arrives.
     *
     * @param message
     */
    void onMessage(KafkaMessage message);
}
