package com.github.ddth.kafka.qnd;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerExample2 {

    public static void main(String[] args) throws Exception {
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String TOPIC = "t1partition";
        // final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();
            kafkaClient.seekToEnd(GROUP_ID, TOPIC);

            final int NUM_ITEMS = 100;
            for (int i = 0; i < NUM_ITEMS; i++) {
                String content = String.valueOf(i) + " / " + GROUP_ID + "-"
                        + DateFormatUtils.toString(new Date(), "yyyy-MM-dd HH:mm:ss");
                KafkaMessage msg = new KafkaMessage(TOPIC, content);
                kafkaClient.sendMessage(msg);
            }
            System.out.println("Num sent: " + NUM_ITEMS);

            Thread.sleep(1000);

            int COUNTER = 0;
            KafkaMessage msg = kafkaClient.consumeMessage(GROUP_ID, CONSUME_FROM_BEGINNING, TOPIC,
                    100, TimeUnit.MILLISECONDS);
            while (msg != null) {
                System.out.println(msg.contentAsString());
                COUNTER++;
                msg = kafkaClient.consumeMessage(GROUP_ID, CONSUME_FROM_BEGINNING, TOPIC, 10,
                        TimeUnit.MILLISECONDS);
            }
            System.out.println("Num received: " + COUNTER);
        }
    }
}
