package com.github.ddth.kafka.qnd;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.github.ddth.kafka.internal.KafkaHelper;

public class QndKafkaConsumerExample {

    public static void main(String[] args) {
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String TOPIC = "demo";
        // final String GROUP_ID = "mynewid-" + System.currentTimeMillis();
        final String GROUP_ID = "myoldid";
        final boolean CONSUME_FROM_BEGINNING = true;

        KafkaConsumer<String, byte[]> consumer = KafkaHelper.createKafkaConsumer(BOOTSTRAP_SERVERS,
                GROUP_ID, CONSUME_FROM_BEGINNING, true, true);

        List<String> topics = Arrays.asList(TOPIC);
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, byte[]> msg = consumer.poll(10);
            for (ConsumerRecord<String, byte[]> cr : msg) {
                System.out.println("==================================================");
                System.out.println(cr);
            }
        }
    }
}
