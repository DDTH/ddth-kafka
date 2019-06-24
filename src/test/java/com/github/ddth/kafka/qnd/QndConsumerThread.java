package com.github.ddth.kafka.qnd;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class QndConsumerThread {

    public static void main(String[] args) throws Exception {
        final Random RAND = new Random(System.currentTimeMillis());
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        //        final String GROUP_ID = "mygroupid";
        final KafkaClient.ProducerType PRODUCER_TYPE = KafkaClient.ProducerType.LEADER_ACK;
        final String TOPIC = "t1partition";

        try (KafkaClient kafkaClient = new KafkaClient(BOOTSTRAP_SERVERS)) {
            kafkaClient.init();

            Thread t = new Thread(() -> {
                while (true) {
                    KafkaMessage msg = kafkaClient.consumeMessage(GROUP_ID, false, TOPIC, 1000, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        System.out.println(msg.contentAsString());
                    }
                }
            });
            t.setDaemon(true);
            t.start();

            for (int i = 0; i < 10; i++) {
                KafkaMessage msg = new KafkaMessage(TOPIC,
                        i + ": " + DateFormatUtils.toString(new Date(), "yyyy-MM-dd HH:mm:ss.SSS"));
                kafkaClient.sendMessage(PRODUCER_TYPE, msg);
                Thread.sleep(RAND.nextInt(1000) + 1);
            }

            Thread.sleep(4000);
        }
    }
}
