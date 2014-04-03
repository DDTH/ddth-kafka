ddth-kafka 
==========

DDTH's Kafka Libraries and Utilities: simplify using [Apache Kafka](http://kafka.apache.org/).

Project home:
[https://github.com/DDTH/ddth-kafka](https://github.com/DDTH/ddth-kafka)

OSGi environment: since v0.2.0 `ddth-kafka` is packaged as an OSGi bundle.


## License ##

See LICENSE.txt for details. Copyright (c) 2014 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Installation #

Latest release version: `0.2.1`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-kafka</artifactId>
	<version>0.2.1</version>
</dependency>
```


## Usage ##

Publish messages:

```java
import com.github.ddth.kafka.KafkaProducer;
...
String kafkaBrokerList = "host1:9092,host2:9092";
KafkaProducer producer = new KafkaProducer(kafkaBrokerList);
producer.init(); //don't forget to initialize the producer

//send a message
producer.sendMessage("topic", "message 1");

//send a message with key
producer.sendMessage("topic", "key", "message 2");

//send many messages at once
String[] messages = new String[]{"msg1", "msg2", "msg3"};
producer.sendMessages("topic", messages);

producer.destroy(); //destroy the producer when done
```

Consume one single message:

```java
import com.github.ddth.kafka.KafkaConsumer;
...
String zookeeperConnString = "host1:2182,host2:2182/kafka";
String consumerGroupId = "my-group-id";
KafkaConsumer consumer = new KafkaConsumer(zookeeperConnString, consumerGroupId);
consumer.init(); //don't forget to initialize the consumer

//consume one message from a topic, this method blocks until message is available
byte[] message = consumer.consume("topic");

//consume one message from a topic, wait up to 3 seconds for message to become available
byte[] message = consumer.consume("topic", 3, TimeUnit.SECONDS);

consumer.destroy(); //destroy the consumer when done
```

Consume messages using message listener:

```java
import com.github.ddth.kafka.KafkaConsumer;
import com.github.ddth.kafka.IKafkaMessageListener;
...
String zookeeperConnString = "host1:2182,host2:2182/kafka";
String consumerGroupId = "my-group-id";
KafkaConsumer consumer = new KafkaConsumer(zookeeperConnString, consumerGroupId);
consumer.init(); //don't forget to initialize the consumer

IKafkaMessageListener messageListener = new IKafkaMessageListener() {
    @Override
    public void onMessage(String topic, int partition, long offset, byte[] key, byte[] message) {
        //do something with the received message
    }
}
consumer.addMessageListener("topic", messageListener);
...
//stop receving message
consumer.removeMessageListener("topic", messageListener);
...

consumer.destroy(); //destroy the consumer when done
```

> #### Consumer Group Id ####
> If two or more comsumers have a same group id, and consume messages from a same topic: messages will be consumed just like a queue: no message is consumed by more than one consumer. Which consumer consumes which message is undetermined.
>
> If two or more comsumers with different group ids, and consume messages from a same topic: messages will be consumed just like publish-subscribe pattern: one message is consumed by all consumers.
