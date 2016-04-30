ddth-kafka 
==========

DDTH's Kafka Libraries and Utilities: simplify using [Apache Kafka](http://kafka.apache.org/).

Project home:
[https://github.com/DDTH/ddth-kafka](https://github.com/DDTH/ddth-kafka)

OSGi environment: `ddth-kafka` is packaged as an OSGi bundle.


## License ##

See LICENSE.txt for details. Copyright (c) 2014-2016 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Installation #

Latest release version: `1.2.1`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-kafka</artifactId>
	<version>1.2.1</version>
</dependency>
```


## Usage ##

**IMPORTANT!**

Since v1.2.0 `ddth-kafka` uses the new version 0.9.x of Kafka producer and consumer.
It does _not_ work with Kafka broker pre-0.9. Please [upgrade your Kafka broker cluster to 0.9.x](http://kafka.apache.org/documentation.html#upgrade)!


**Initialize a Kafka client:**

```java
import com.github.ddth.kafka.KafkaClient;
...
String bootstrapServers = "localhost:9092";
KafkaClient kafkaClient = new KafkaClient(bootstrapServers);
kafkaClient.init();
```

**Publish message(s):**

```java
import com.github.ddth.kafka.KafkaMessage;
...
KafkaMessage msg = new KafkaMessage("topic", "message-content-1");
kafkaClient.sendMessage(msg);

/*
 * Messages with same key will be put into a same partition.
 */ 
msg = new KafkaMessage("topic", "msg-key", "message-content-2");
kafkaClient.sendMessage(msg);
```

**Consume one single message:**

```java
final String consumerGroupId = "my-group-id";
final boolean consumeFromBeginning = true;
final String topic = "topic-name";

//consume one message from a topic
KafkaMessage msg = consumer.consumeMessage(consumerGroupId, consumeFromBeginning, topic);

//consume one message from a topic. This is shorthand for consumer.consumeMessage(consumerGroupId, true, topic);
KafkaMessage msg = consumer.consumeMessage(consumerGroupId, topic);

//consume one message from a topic, wait up to 3 seconds
KafkaMessage msg = consumer.consumeMessage(consumerGroupId, consumeFromBeginning, topic, 3, TimeUnit.SECONDS);

//consume one message from a topic, wait up to 3 seconds. This is shorthand for consumer.consumeMessage(consumerGroupId, true, topic, 3, TimeUnit.SECONDS);
KafkaMessage msg = consumer.consumeMessage(consumerGroupId, topic, 3, TimeUnit.SECONDS);
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

//stop receving message
kafkaClient.removeMessageListener(consumerGroupId, topic, messageListener);
```

**Do NOT use one KafkaClient to consume messages both one-by-one and by message-listener**
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
