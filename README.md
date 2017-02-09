ddth-kafka 
==========

DDTH's Kafka Libraries and Utilities: simplify [Apache Kafka](http://kafka.apache.org/) usage.

Project home:
[https://github.com/DDTH/ddth-kafka](https://github.com/DDTH/ddth-kafka)

**ddth-kafka requires Java 8+ since v1.3.0**


## License ##

See LICENSE.txt for details. Copyright (c) 2014-2017 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Installation ##

Latest release version: `1.3.2`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-kafka</artifactId>
	<version>1.3.2</version>
</dependency>
```


## Usage ##

**IMPORTANT!**

Since v1.3.0 `ddth-kafka` uses the new version 0.10.x of Kafka producer and consumer.
It may _not_ work with old Kafka brokers. [Upgrade your Kafka broker cluster](http://kafka.apache.org/documentation.html#upgrade)!

### Work with Kafka API directly ###

***Producer***

See [Kafka documentation](https://kafka.apache.org/documentation/#producerconfigs) for more information.

```java
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.github.ddth.kafka.KafkaClient.ProducerType;
...
ProducerType producerType = ProducerType.SYNC_LEADER_ACK;
String bootstrapServers = "localhost:9092";
Properties customProperties = null;
KafkaProducer<String, byte[]> producer = KafkaHelper.createKafkaProducer(
    producerType, 
    bootstrapServers, 
    customProperties);
...
// example: send a message
ProducerRecord<String, byte[]> record = new ProducerRecord<>("topic", "content".getBytes());
Future<RecordMetadata> result = producer.send(record);
...
// close producer when done
producer.close();
```

***Consumer***

See [Kafka documentation](https://kafka.apache.org/documentation/#newconsumerconfigs) for more information.

```java
import org.apache.kafka.clients.consumer.KafkaConsumer;
...
String bootstrapServers = "localhost:9092";
String groupId = "my-consumer-groupid";
boolean consumeFromBeginning = true;
boolean autoCommitOffsets = true;
boolean leaderAutoRebalance = true;
boolean customProperties = null;
KafkaConsumer<String, byte[]> consumer = KafkaHelper.createKafkaConsumer(
    bootstrapServers, 
    groupId, 
    consumeFromBeginning,
    autoCommitOffsets, 
    leaderAutoRebalance,
    customProperties);
...
// consume messages
while ( running ) {
    ConsumerRecords<String, byte[]> records = consumer.poll(1000);
    for (ConsumerRecord<String, byte[]> record : records) {
        processMessage(record);
    }
    
    /* if not autoCommitOffsets, commit offsets manually */
    if ( !autoCommitOffsets )
        try {
            consumer.commitSync();
        } catch (CommitFailedException e) {
            //handle exception
        }
}
...
// close consumer when done
consumer.close();
```

### Work with Kafka API via KafkaClient ###

**Initialize a Kafka client:**

```java
import com.github.ddth.kafka.KafkaClient;
...
String bootstrapServers = "localhost:9092";
KafkaClient kafkaClient = new KafkaClient(bootstrapServers);
kafkaClient.init();
```

**Send message(s):**

```java
import com.github.ddth.kafka.KafkaMessage;
...
// messages with null or empty key will be put into random partition
KafkaMessage msg = new KafkaMessage("topic", "message-content");
KafkaMessage result = kafkaClient.sendMessage(msg);

// messages with same key will be put into a same partition.
KafkaMessage msg = new KafkaMessage("topic", "msg-key", "message-content");
KafkaMessage result = kafkaClient.sendMessage(msg);
```

_Upon successful, `KafkaClient.sendMessage(KafkaMessage)` returns a copy of the sent message filled with partition number and offset._

Send message(s) asynchronously:

```java
import java.util.concurrent.Future;
import com.github.ddth.kafka.KafkaMessage;
...
Future<KafakMessage> result = kafkaClient.sendMessageAsync(msg);
```

Or, send message(s) asynchronously and get the result from Kafka API:

```java
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.github.ddth.kafka.KafkaMessage;
...
Future<RecordMetadata> result = kafkaClient.sendMessageRaw(msg);

//or with callback handler
kafkaClient.sendMessageRaw(msg, new Callback() {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            // handle exception
        } else {
            // message sent successfully
        }
    }
});
```

**Consume one single message:**

```java
final String consumerGroupId = "my-group-id";
final boolean consumeFromBeginning = true;
final String topic = "topic-name";

//consume one message from a topic
KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, consumeFromBeginning, topic);

//consume one message from a topic. This is shorthand for kafkaClient.consumeMessage(consumerGroupId, true, topic);
KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, topic);

//consume one message from a topic, wait up to 3 seconds
KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, consumeFromBeginning, topic, 3, TimeUnit.SECONDS);

//consume one message from a topic, wait up to 3 seconds. This is shorthand for kafkaClient.consumeMessage(consumerGroupId, true, topic, 3, TimeUnit.SECONDS);
KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, topic, 3, TimeUnit.SECONDS);
```

**Consume messages using message listener:**

```java
import com.github.ddth.kafka.IKafkaMessageListener;
...
IKafkaMessageListener messageListener = new IKafkaMessageListener() {
    @Override
    public void onMessage(KafkaMessage message) {
        //do something with the received message
    }
}
kafkaClient.addMessageListener(consumerGroupId, consumeFromBeginning, topic, msgListener);

//stop receving messages
kafkaClient.removeMessageListener(consumerGroupId, topic, messageListener);
```

**Seek to a specific offset:***

```java
// seek to beginning of all assigned partitions of a topic.
kafkaClient.seekToBeginning(consumerGroupId, topic);

// seek to end of all assigned partitions of a topic.
kafkaClient.seekToEnd(consumerGroupId, topic);

// seek to a specific position
String topic = "topic-name";
int partition = 0;
long offset = 100;
kafkaClient.seek(consumerGroupId, new KafkaTopicPartitionOffset(topic, partition, offset));
```

**Commit offsets manually of autoCommitOffsets is set to false:***

```java
// commit offset for the last consumed message
KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, topic);
kafkaClient.commit(msg);

// commit offsets returned on the last poll for all the subscribed partitions.
kafkaClient.commit(topic, groupId);

// commit the specified offsets for the specified list of topics and partitions.
kafkaClient.commit(groupId,
    new KafkaTopicPartitionOffset(topic1, partition1, offset1),
    new KafkaTopicPartitionOffset(topic2, partition2, offset2),
    new KafkaTopicPartitionOffset(topic3, partition3, offset3)
);
```

**Do NOT use a same KafkaClient to consume messages both one-by-one and by message-listener**
Create a KafkaClient to consume messages one-by-one, and create another client (with different group-id) to consume messages using listener.


**Destroy Kafka client when done:**

```java
kafkaClient.destroy();
```

> #### Consumer Group Id ####
> Each consumer is associated with a consumer-group-id. Kafka remembers offset of the last message that the consumer has consumed. Thus, consumer can resume from where it has left.
>
> The very first time a consumer consumes a message from an existing topic, it can choose to consume messages from the beginning (i.e. consume messages that already exist in the topic), or consume only new messages (e.g. existing messages in the topic are ignored). This is controlled by the `consume-from-beginning` parameter.
> 
> If two or more comsumers have same one group id, and consume messages from a same topic: messages will be consumed just like a queue: no message is consumed by more than one consumer. Which consumer consumes which message is undetermined.
>
> If two or more comsumers with different group ids, and consume messages from a same topic: messages will be consumed just like publish-subscribe pattern: one message is consumed by all consumers.
