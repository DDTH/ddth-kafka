package com.github.ddth.kafka.qnd;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;

import com.github.ddth.kafka.KafkaClient;

public class BaseQndKafka {

    protected void createTopic(TestingServer zkServer, String topic) {
        ZkClient zkClient = new ZkClient(zkServer.getConnectString(), 30000, 30000,
                ZKStringSerializer$.MODULE$);
        Properties props = new Properties();
        int partitions = 1;
        int replicationFactor = 1;
        AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, props);
        zkClient.close();
    }

    protected TestingServer newZkServer() throws Exception {
        TestingServer zkServer = new TestingServer();
        return zkServer;
    }

    protected KafkaServerStartable newKafkaServer(TestingServer zkServer) throws Exception {
        KafkaConfig config = getKafkaConfig(zkServer.getConnectString());
        KafkaServerStartable kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();
        return kafkaServer;
    }

    // protected KafkaProducer newKafkaProducer(KafkaServerStartable
    // kafkaServer,
    // KafkaProducer.Type type) throws Exception {
    // KafkaProducer kafkaProducer = new
    // KafkaProducer(getKafkaBrokerString(kafkaServer), type);
    // kafkaProducer.init();
    // return kafkaProducer;
    // }
    //
    // protected KafkaConsumer newKafkaConsumer(TestingServer zkServer, String
    // groupId)
    // throws Exception {
    // KafkaConsumer kafkaConsumer = new
    // KafkaConsumer(getZkConnectString(zkServer), groupId);
    // kafkaConsumer.init();
    // return kafkaConsumer;
    // }

    protected KafkaClient newKafkaClient(TestingServer zkServer) throws Exception {
        KafkaClient kafkaClient = new KafkaClient(getZkConnectString(zkServer));
        kafkaClient.init();
        return kafkaClient;
    }

    // protected String getKafkaBrokerString(KafkaServerStartable kafkaServer) {
    // return String.format("localhost:%d", kafkaServer.serverConfig().port());
    // }

    protected String getZkConnectString(TestingServer zkServer) {
        return zkServer.getConnectString();
    }

    // protected int getKafkaPort(KafkaServerStartable kafkaServer) {
    // return kafkaServer.serverConfig().port();
    // }
    //
    // protected String getKafkaHost(KafkaServerStartable kafkaServer) {
    // return kafkaServer.serverConfig().hostName();
    // }

    private static KafkaConfig getKafkaConfig(final String zkConnectString) {
        scala.collection.Iterator<Properties> propsI = TestUtils.createBrokerConfigs(1, true).iterator();
        assert propsI.hasNext();
        Properties props = propsI.next();
        assert props.containsKey("zookeeper.connect");
        props.put("zookeeper.connect", zkConnectString);
        props.put("num.partitions", "2");
        props.put("auto.create.topics.enable", "false");
        return new KafkaConfig(props);
    }
}
