package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndKafkaConsumerMessageListener {

    public void qndMessageListener() throws Exception {
        try (KafkaClient kafkaClient = new KafkaClient("localhost:9092")) {
            kafkaClient.init();

            final String consumerGroupId = "mygroupid";
            final String topic = "demo";

            kafkaClient.addMessageListener(consumerGroupId, true, topic,
                    new IKafkaMessageListener() {
                        @Override
                        public void onMessage(KafkaMessage message) {
                            System.out.println(message != null ? message.contentAsString() : null);
                        }
                    });

            for (int i = 0; i < 10; i++) {
                KafkaMessage msg = new KafkaMessage(topic,
                        "message - " + i + ": " + System.currentTimeMillis());
                kafkaClient.sendMessage(msg);
            }
            Thread.sleep(2000);
        }
    }

    public static void main(String[] args) throws Exception {
        QndKafkaConsumerMessageListener test = new QndKafkaConsumerMessageListener();
        test.qndMessageListener();
    }
}
