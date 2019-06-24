package com.github.ddth.kafka.qnd;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.kafka.KafkaClient.ProducerType;
import com.github.ddth.kafka.internal.KafkaHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

public class QndTestTopicSeekBeginning {

    public static void main(String[] args) throws Exception {
        final Random RANDOM = new Random(System.currentTimeMillis());
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String TOPIC = "ddth-kafka";
        final String GROUP_ID = "mynewid" + System.currentTimeMillis();
        final boolean CONSUME_FROM_BEGINNING = false;
        boolean stopped = false;

        try (KafkaProducer<String, byte[]> producer = KafkaHelper
                .createKafkaProducer(ProducerType.LEADER_ACK, BOOTSTRAP_SERVERS)) {
            Thread producerThread = new Thread(() -> {
                while (!stopped) {
                    String value = DateFormatUtils.toString(new Date(), "yyyy-MM-dd HH:mm:ss");
                    ProducerRecord<String, byte[]> pr = new ProducerRecord<>(TOPIC, value.getBytes());
                    try {
                        System.out.println("Send: " + producer.send(pr).get() + " / " + value);
                        Thread.sleep(RANDOM.nextInt(1024));
                    } catch (Exception e) {
                    }
                }
            });
            producerThread.setDaemon(true);
            producerThread.start();

            try (KafkaConsumer<String, byte[]> consumer = KafkaHelper
                    .createKafkaConsumer(BOOTSTRAP_SERVERS, GROUP_ID, CONSUME_FROM_BEGINNING,
                            true /* auto-commit offset*/)) {
                consumer.subscribe(Arrays.asList(TOPIC));

                for (int i = 0; i < 5; i++) {
                    Thread.sleep(RANDOM.nextInt(10000));
                    System.err.println(
                            "Seek to beginning [" + TOPIC + "]: " + KafkaHelper.seekToBeginning(consumer, TOPIC));
                    ConsumerRecords<?, byte[]> crs = consumer.poll(Duration.ofMillis(1000));
                    System.err.println("\tCount: " + crs.count());
                    int num = 0;
                    for (ConsumerRecord<?, byte[]> cr : crs) {
                        System.out.println(
                                "\t{t:" + cr.topic() + ", p:" + cr.partition() + ", o:" + cr.offset() + ", v:"
                                        + new String(cr.value()) + "}");
                        num++;
                        if (num > 2) {
                            break;
                        }
                    }
                }
            }
        }
    }
}
