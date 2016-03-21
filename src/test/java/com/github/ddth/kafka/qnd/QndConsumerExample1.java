package com.github.ddth.kafka.qnd;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

public class QndConsumerExample1 {

    public static void main(String[] args) throws Exception {
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String TOPIC = "t1partition";
        final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        // final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;

        boolean messageConsumed = false;

        KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS);
        try {
            kafkaClient.init();
            kafkaClient.seekToEnd(GROUP_ID, TOPIC);

            for (int i = 0; i < 10; i++) {
                System.out.println("LOOP: ==[" + i + "]==");
                KafkaMessage msg = new KafkaMessage(TOPIC, String.valueOf(i) + " / " + GROUP_ID
                        + "-" + DateFormatUtils.toString(new Date(), "yyyy-MM-dd HH:mm:ss"));
                kafkaClient.sendMessage(msg);

                // msg = kafkaClient.consumeMessage(GROUP_ID,
                // CONSUME_FROM_BEGINNING, TOPIC);
                msg = kafkaClient.consumeMessage(GROUP_ID, CONSUME_FROM_BEGINNING, TOPIC,
                        messageConsumed ? 10 : 100, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    System.out.println("\tRECEIVED message: " + msg.contentAsString());
                    messageConsumed = true;
                } else {
                    System.out.println("\tRECEIVED message: [null]");
                }
                // System.out.println();
            }
        } finally {
            kafkaClient.destroy();
        }
    }

}
