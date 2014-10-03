ddth-kafka release notes
========================

1.0.2 - 2014-10-03
------------------
- Bug fixed.


1.0.1 - 2014-10-02
------------------
- `KafkaClient`: force to use a random partitioner (class `RandomPartitioner`) by default.


1.0.0 - 2014-08-23
------------------
- `v1.0.0` released!
- Breaking API changes: new class `KafkaClient` replaces both `KafkaConsumer` and `KafkaProducer`.
- New class `KafkaMessage`, used by both sending and receiving.


0.2.1.1 - 2014-07-29
--------------------
- When there are 2 or more `IKafkaMessageListener`s on same topic, each `IKafkaMessageListener.onMessage` is handled by a separated thread. This would boost the consumer's performance a bit. 


0.2.1 - 2014-04-03
------------------
- `auto.offset.reset` set to `largest` by default. Add option to consume messages from beginning. (See more: [http://kafka.apache.org/08/configuration.html](http://kafka.apache.org/08/configuration.html).


0.2.0 - 2014-03-19
------------------
- Merged with [ddth-osgikafka](https://github.com/DDTH/ddth-osgikafka) and packaged as OSGi bundle.


0.1.0 - 2014-02-21
------------------
- First release.
