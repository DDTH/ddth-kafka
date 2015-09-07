package com.github.ddth.kafka.qnd;

import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerExample {

    public static void main(String[] args) throws Exception {
        final String zkConnString = "localhost:2181/kafka";
        final String topic = "testtopic";
        final String consumerGroupId = "group-id-1";

        KafkaClient kafkaClient = new KafkaClient(zkConnString);
        kafkaClient.init();

        for (int i = 0; i < 11; i++) {
            KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 3000,
                    TimeUnit.MILLISECONDS);
            // System.out.println(msg);
            if (msg != null) {
                System.out.println(msg.contentAsString());
            } else {
                System.out.println("null");
            }
            System.out.println();
        }

        kafkaClient.destroy();
    }

}
