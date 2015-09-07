package com.github.ddth.kafka;

import com.github.ddth.kafka.internal.KafkaConsumer;



/**
 * Abstract implementation of {@link IKafkaMessageListener}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class AbstractKafkaMessagelistener implements IKafkaMessageListener {

    private KafkaConsumer kafkaConsumer;

    public AbstractKafkaMessagelistener(String topic, KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    protected KafkaConsumer getKafkaConsumer() {
        return kafkaConsumer;
    }
}
