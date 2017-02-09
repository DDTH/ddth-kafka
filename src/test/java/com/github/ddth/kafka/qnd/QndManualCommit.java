package com.github.ddth.kafka.qnd;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.github.ddth.kafka.internal.KafkaHelper;

public class QndManualCommit {

    public static void main(String[] args) {
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String TOPIC = "ddth-kafka";
        final String GROUP_ID = "ddth-kafka";

        KafkaConsumer<String, byte[]> consumer = KafkaHelper.createKafkaConsumer(BOOTSTRAP_SERVERS,
                GROUP_ID, true, false, true);
        try {
            consumer.subscribe(Arrays.asList(TOPIC));

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                for (ConsumerRecord<String, byte[]> cr : records) {
                    String output = MessageFormat.format(
                            "Topic: {0}, Partition: {1}, Offset: {2}, Content: {3}", cr.topic(),
                            cr.partition(), cr.offset(), new String(cr.value()));
                    System.out.println(output);
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(new TopicPartition(TOPIC, cr.partition()),
                            new OffsetAndMetadata(cr.offset() + 1));
                    consumer.commitSync(offsets);
                }
            }
        } finally {
            consumer.close();
        }
    }

}
