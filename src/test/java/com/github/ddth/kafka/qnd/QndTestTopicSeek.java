package com.github.ddth.kafka.qnd;

import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.kafka.KafkaClient.ProducerType;
import com.github.ddth.kafka.internal.KafkaHelper;

public class QndTestTopicSeek {
    public static void main(String[] args) throws Exception {
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String TOPIC = "demo";
        // final String GROUP_ID = "mynewid" + System.currentTimeMillis();
        final String GROUP_ID = "myoldgroupid";
        final boolean CONSUME_FROM_BEGINNING = true;

        try (KafkaProducer<String, byte[]> producer = KafkaHelper
                .createKafkaProducer(ProducerType.FULL_ASYNC, BOOTSTRAP_SERVERS)) {
            try (KafkaConsumer<String, byte[]> consumer = KafkaHelper.createKafkaConsumer(
                    BOOTSTRAP_SERVERS, GROUP_ID, CONSUME_FROM_BEGINNING, true, true)) {
                KafkaHelper.seekToEnd(consumer, TOPIC);

                String value = DateFormatUtils.toString(new Date(), "yyyy-MM-dd HH:mm:ss");
                ProducerRecord<String, byte[]> pr = new ProducerRecord<String, byte[]>(TOPIC,
                        value.getBytes());
                System.out.println("Send: " + producer.send(pr).get());

                consumer.subscribe(Arrays.asList(TOPIC));
                ConsumerRecords<?, byte[]> crs = consumer.poll(1000);
                System.out.println("Count: " + crs.count());
                int num = 0;
                for (ConsumerRecord<?, byte[]> cr : crs) {
                    System.out.println(cr.topic() + "/" + cr.partition() + "/" + cr.offset() + "/"
                            + new String(cr.value()));
                    num++;
                    if (num > 2) {
                        break;
                    }
                }

                System.out.println("==================================================");

                KafkaHelper.seekToBeginning(consumer, TOPIC);

                value = DateFormatUtils.toString(new Date(), "yyyy-MM-dd HH:mm:ss");
                pr = new ProducerRecord<String, byte[]>(TOPIC, value.getBytes());
                System.out.println("Send: " + producer.send(pr).get());

                // consumer.subscribe(Arrays.asList(TOPIC));
                crs = consumer.poll(1000);
                System.out.println("Count: " + crs.count());
                num = 0;
                for (ConsumerRecord<?, byte[]> cr : crs) {
                    System.out.println(cr.topic() + "/" + cr.partition() + "/" + cr.offset() + "/"
                            + new String(cr.value()));
                    num++;
                    if (num > 2) {
                        break;
                    }
                }
            }
        }
    }
}
