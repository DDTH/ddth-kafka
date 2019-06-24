package com.github.ddth.kafka.qnd;

import com.github.ddth.kafka.internal.KafkaHelper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/*
 * mvn package exec:java -Dexec.classpathScope=test -Dexec.mainClass="com.github.ddth.kafka.qnd.QndTestConsumeWithoutCommit" -Dbrokers=localhost:9092 -Dtopic=topic -Dgroup=group-id-1
 */
public class QndTestConsumeWithoutCommit {

    /**
     * Run this class twice or three times.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final String brokers = System.getProperty("brokers", "localhost:9092");
        final String topic = System.getProperty("topic", "t1partition");
        final String grId = System.getProperty("group", "mygroupid1");

        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 128);

        KafkaConsumer<String, byte[]> consumer = KafkaHelper.createKafkaConsumer(brokers, grId, true, false, props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, byte[]> recs = consumer.poll(Duration.ofMillis(100));
            recs.forEach(x -> System.out.println(
                    "Consumer1: " + x.topic() + " / " + x.partition() + " / " + x.offset() + " / " + new String(
                            x.value())));
            System.out.println("Sleeping...");
            Thread.sleep(1000);
        }
    }
}
