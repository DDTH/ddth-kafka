package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndProducerExample {

    public static void main(String[] args) throws Exception {
        final String zkConnString = "localhost:2181/kafka";
        final String topic = "testtopic";

        KafkaClient kafkaClient = new KafkaClient(zkConnString);
        kafkaClient.init();

        for (int i = 0; i < 100; i++) {
            byte[] content = String.valueOf(System.currentTimeMillis()).getBytes();
            kafkaClient.sendMessage(new KafkaMessage().topic(topic).content(content));
        }

        kafkaClient.destroy();
    }

}
