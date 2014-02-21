package com.github.ddth.kafka;

/**
 * Kafka message listener interface.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface IKafkaMessageListener {
    /**
     * Called when a message arrives.
     * 
     * @param topic
     * @param partition
     * @param offset
     * @param key
     * @param message
     */
    public void onMessage(String topic, int partition, long offset, byte[] key, byte[] message);
}
