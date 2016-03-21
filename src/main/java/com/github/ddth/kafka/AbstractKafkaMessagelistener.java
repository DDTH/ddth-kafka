package com.github.ddth.kafka;

import com.github.ddth.kafka.internal.KafkaMsgConsumer;



/**
 * Abstract implementation of {@link IKafkaMessageListener}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class AbstractKafkaMessagelistener implements IKafkaMessageListener {

    private KafkaMsgConsumer kafkaConsumer;

    public AbstractKafkaMessagelistener(String topic, KafkaMsgConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    protected KafkaMsgConsumer getKafkaConsumer() {
        return kafkaConsumer;
    }
}
