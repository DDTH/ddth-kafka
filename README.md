ddth-kafka 
==========

DDTH's Kafka Libraries and Utilities: simplify using [Apache Kafka](http://kafka.apache.org/).

Project home:
[https://github.com/DDTH/ddth-kafka](https://github.com/DDTH/ddth-kafka)

For OSGi environment, see [ddth-osgikafka](https://github.com/DDTH/ddth-osgikafka).


## License ##

See LICENSE.txt for details. Copyright (c) 2014 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Installation #

Latest release version: `0.1.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-kafka</artifactId>
	<version>0.1.0</version>
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

producer.destroy; //destroy the producer when done
```

Consume single message:

```java
import com.github.ddth.kafka.KafkaConsumer;
...
String zookeeperConnString = "host1:2182,host2:2182/kafka";
String consumerGroupId = "my-group-id";
KafkaConsumer consumer = new KafkaConsumer(zookeeperConnString, consumerGroupId);
consumer.init(); //don't forget to initialize the consumer

consumer.destroy(); //destroy the consumer when done
