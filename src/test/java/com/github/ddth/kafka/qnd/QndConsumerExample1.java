package com.github.ddth.kafka.qnd;

import java.util.concurrent.TimeUnit;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerExample1 {

    public static void main(String[] args) throws Exception {
        final String zkConnString = "localhost:2181/kafka";
        final String topic = "t1partition";
        final String consumerGroupId = "group-id-1";

        KafkaClient kafkaClient = new KafkaClient(zkConnString);
        try {
            kafkaClient.init();
            for (int i = 0; i < 11; i++) {
                KafkaMessage msg = new KafkaMessage(topic, String.valueOf(i).getBytes());
                kafkaClient.sendMessage(msg);

                msg = kafkaClient.consumeMessage(consumerGroupId, true, topic, 1000,
                        TimeUnit.MILLISECONDS);
                if (msg != null) {
                    System.out.println(msg.contentAsString());
                } else {
                    System.out.println("null");
                }
            }
        } finally {
            kafkaClient.destroy();
        }
    }

}
