package com.github.ddth.kafka.test;

import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

import com.github.ddth.kafka.KafkaClient;

/**
 * Unit test for simple App.
 */
public abstract class BaseKafkaTest extends TestCase {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public BaseKafkaTest(String testName) {
        super(testName);
    }

    protected KafkaServerStartable kafkaServer;
    protected TestingServer zkServer;

    protected KafkaClient kafkaClient;

    protected void createTopic(String topic) throws Exception {
        ZkClient zkClient = new ZkClient(zkServer.getConnectString(), 30000, 30000,
                ZKStringSerializer$.MODULE$);
        try {
            Properties props = new Properties();
            int partitions = 4;
            int replicationFactor = 1;
            AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, props);
            Thread.sleep(2000);
        } finally {
            zkClient.close();
        }
    }

    private static KafkaConfig getKafkaConfig(final String zkConnectString) {
        scala.collection.Iterator<Properties> propsI = TestUtils.createBrokerConfigs(1, true)
                .iterator();
        assert propsI.hasNext();
        Properties props = propsI.next();
        assert props.containsKey("zookeeper.connect");
        props.put("zookeeper.connect", zkConnectString);
        props.put("num.partitions", "4");
        props.put("auto.create.topics.enable", "true");
        return new KafkaConfig(props);
    }

    @Before
    public void setUp() throws Exception {
        // setup Zookeeper
        zkServer = new TestingServer();

        // setup Kafka
        KafkaConfig config = getKafkaConfig(zkServer.getConnectString());
        kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();

        kafkaClient = new KafkaClient(getZkConnectString());
        kafkaClient.init();
    }

    @After
    public void tearDown() throws IOException {
        kafkaClient.destroy();

        kafkaServer.shutdown();
        zkServer.stop();
        zkServer.close();
    }

    protected String getKafkaBrokerString() {
        return String.format("localhost:%d", kafkaServer.serverConfig().port());
    }

    protected String getZkConnectString() {
        return zkServer.getConnectString();
    }

    protected int getKafkaPort() {
        return kafkaServer.serverConfig().port();
    }

    protected String getKafkaHost() {
        return kafkaServer.serverConfig().hostName();
    }
}
