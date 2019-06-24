# ddth-kafka

DDTH's Kafka utility library to simplify [Apache Kafka](http://kafka.apache.org/) usage.

Project home:
[https://github.com/DDTH/ddth-kafka](https://github.com/DDTH/ddth-kafka)

**`ddth-kafka` requires Java 11+ since v2.0.0, for Java 8, use v1.3.x**


## Installation

Latest release version: `2.0.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-kafka</artifactId>
	<version>2.0.0</version>
</dependency>
```


## Usage

**IMPORTANT!**

`ddth-kafka:2.0.0` uses `org.apache.kafka:kafka-clients:2.2.1` and is tested against `KafkaServer_2.12-2.2.1`.
It may _not_ work with old Kafka brokers. [Upgrade](http://kafka.apache.org/documentation.html#upgrade) your Kafka broker cluster if needed.

**Create and initialize `KafkaClient` instance**

```java
import com.github.ddth.kafka.KafkaClient;
import java.util.Properties;

String bootstrapServers = "localhost:9092;node2:port;node3:port";
KafkaClient kafkaClient = new KafkaClient(bootstrapServers);

//custom configurations for Kafka producers
//Properties customProducerProps = ...
//kafkaClient.setProducerProperties(customProducerProps);

//custom configurations for Kafka consumers
//Properties customConsumerProps = ...
//kafkaClient.setConsumerProperties(customConsumerProps);

kafkaClient.init();
```

`KafkaClient` uses a default configuration set for Kafka producers and consumers. However, the configuration can be overridden by
`KafkaClient.setProducerProperties(Properties)` and `KafkaClient.setConsumerProperties(Properties)`.
Custom configurations are merged with the default one: if a custom setting exists, it overrides the default one.

See [Kafka documentations](https://kafka.apache.org/documentation/) for [Producer](https://kafka.apache.org/documentation/#producerconfigs) and [Consumer](https://kafka.apache.org/documentation/#consumerconfigs) configs.


**Send/Publish messages**

```java
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

//prepare the message to be sent
String topic = "topic-name";
String content = "This is a message";
//or: byte[] content = { 0, 2, 3, 4, 5, 6, 7, 8, 9 };
KafkaMessage msg = new KafkaMessage(topic, content);
// or to specify message key for partitioning: KafkaMessage msg = new KafkaMessage(topic, key, content);

//send message synchronously, using the default producer type
KafkaMessage result = kafkaClient.sendMessage(msg);
//or use a specific producer type
KafkaMessage result = kafkaClient.sendMessage(KafkaClient.ProducerType.ALL_ACKS, msg);


//send message asynchronously
Future<KafkaMessage> result = kafkaClient.sendMessageAsync(msg);
//or: Future<KafkaMessage> result = kafkaClient.sendMessageAsync(KafkaClient.ProducerType.NO_ACK, msg);


//send message asynchronously, and receive raw result from Kafka's Java client
Future<org.apache.kafka.clients.producer.RecordMetadata> result = kafkaClient.sendMessageRaw(msg);
//some overload methods:
Future<org.apache.kafka.clients.producer.RecordMetadata> result = kafkaClient.sendMessageRaw(KafkaClient.ProducerType.LEADER_ACK, msg);
org.apache.kafka.clients.producer.Callback callback = ...
Future<org.apache.kafka.clients.producer.RecordMetadata> result = kafkaClient.sendMessageRaw(msg, callback);
Future<org.apache.kafka.clients.producer.RecordMetadata> result = kafkaClient.sendMessageRaw(KafkaClient.ProducerType.LEADER_ACK, msg, callback);


//send a batch of messages
List<KafkaMessage> buffer = ...
List<KafkaMessage> result = kafkaClient.sendBulk(buffer.toArray(KafkaMessage.EMPTY_ARRAY));
//or: List<KafkaMessage> result = kafkaClient.sendBulk(KafkaClient.ProducerType.ALL_ACKS, buffer.toArray(KafkaMessage.EMPTY_ARRAY));
```

There are 3 producer types:
- `KafkaClient.ProducerType.NO_ACK`: producer will not wait for any acknowledgment from the server at all, retries configuration will not take effect. Lowest latency but the weakest durability guarantees.
- `KafkaClient.ProducerType.LEADER_ACK`: leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. Balance latency/durability. This is also the default producer type used by `KafkaClient`.
- `KafkaClient.ProducerType.ALL_ACKS`: leader will wait for the full set of in-sync replicas to acknowledge the record. Best durability but the highest latency.

`KafkaClient.sendBulk(...)` will _not_ send messages using transaction. Some messages may fail while others success.

**Consume messages**

```java
String consumerGroupId = "my-group-id";
String topicName = "my-topic";

//consume one single message (one-by-one)
KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, topicName);

//consume messages using message listener
IKafkaMessageListener msgListener = (msg) -> System.out.println(message.contentAsString());
boolean consumeFromBeginning = true;
kafkaClient.addMessageListener(consumerGroupId, consumeFromBeginning, topicName, messageListener);
//remove message listener to stop consuming: 
//kafkaClient.removeMessageListener(consumerGroupId, topicName, messageListener)
```

Consumer group-id:
- Each consumer is associated with a consumer group-id. Kafka remembers offset of the last message that the consumer has consumed. Thus, consumer can resume from where it has left.
- The very first time a consumer consumes a message from an existing topic, it can choose to consume messages from the beginning (i.e. consume messages that already exist in the topic), or consume only new messages (e.g. existing messages in the topic are ignored). This is controlled by the `consumeFromBeginning` parameter.
- If two or more consumers have a same one group id, and consume messages from a same topic: messages will be consumed just like a queue, no message is consumed by more than one consumer. Which consumer consumes which message is undetermined.
- If two or more consumers with different group ids consume messages from a same topic: messages will be consumed just like publish-subscribe pattern, one message is consumed by all consumers.

_Do NOT use a same `KafkaClient` to consume messages both one-by-one and via message-listener_. Create a `KafkaClient` to consume messages one-by-one, and create another client (_with different group-id_) to consume messages using listener.

**Examples**

More examples code in [src/test/java/com/github/ddth/kafka/qnd/](src/test/java/com/github/ddth/kafka/qnd/).


## License

See [LICENSE.txt](LICENSE.txt) for details. Copyright (c) 2014-2019 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.
