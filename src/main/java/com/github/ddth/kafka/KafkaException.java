package com.github.ddth.kafka;

/**
 * Throwns to indicate there has been an exception while interacting with Kafka.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class KafkaException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public KafkaException() {
    }

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(Throwable throwable) {
        super(throwable);
    }

    public KafkaException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
