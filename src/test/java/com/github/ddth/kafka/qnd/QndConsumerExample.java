package com.github.ddth.kafka.qnd;

import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerExample {

    public static void main(String[] args) throws Exception {
        final String zkConnString = "localhost:2181/kafka";
        final String topic = "testtopic";
        final String consumerGroupId = "group-id-" + System.currentTimeMillis();

        KafkaClient kafkaClient = new KafkaClient(zkConnString);
        kafkaClient.init();

        KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 5000,
                TimeUnit.MILLISECONDS);

        System.out.println(msg);

        kafkaClient.destroy();
    }

}
