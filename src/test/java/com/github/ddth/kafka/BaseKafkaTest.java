package com.github.ddth.kafka;

import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;
import kafka.admin.AdminUtils;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

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
    protected ZkClient zkClient;
    protected SimpleConsumer kafkaSimpleConsumer;

    protected KafkaProducer kafkaProducer;
    protected KafkaConsumer kafkaConsumer;

    protected void createTopic(String topic) {
        Properties props = new Properties();
        int partitions = 1;
        int replicationFactor = 1;
        AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, props);
        // List<KafkaServer> servers = new ArrayList<KafkaServer>();
        // servers.add(kafkaServer);
        // TestUtils.waitUntilMetadataIsPropagated(
        // scala.collection.JavaConversions.asScalaBuffer(servers), TOPIC, 0,
        // 5000);
    }

    private static KafkaConfig getKafkaConfig(final String zkConnectString) {
        scala.collection.Iterator<Properties> propsI = TestUtils.createBrokerConfigs(1).iterator();
        assert propsI.hasNext();
        Properties props = propsI.next();
        assert props.containsKey("zookeeper.connect");
        props.put("zookeeper.connect", zkConnectString);
        props.put("num.partitions", "2");
        props.put("auto.create.topics.enable", "true");
        return new KafkaConfig(props);
    }

    @Before
    public void setUp() throws Exception {
        // setup Zookeeper
        zkServer = new TestingServer();
        zkClient = new ZkClient(zkServer.getConnectString(), 30000, 30000,
                ZKStringSerializer$.MODULE$);

        // setup Kafka
        KafkaConfig config = getKafkaConfig(zkServer.getConnectString());
        kafkaServer = new KafkaServerStartable(config);
        kafkaServer.startup();
        kafkaSimpleConsumer = new SimpleConsumer(getKafkaHost(), getKafkaPort(),
                10000 /* soTimeout */, 64 * 1024 /* */, "clientname");

        // setup Producer
        kafkaProducer = new KafkaProducer(getKafkaBrokerString());
        kafkaProducer.init();

        // setup Consumer
        kafkaConsumer = new KafkaConsumer(getZkConnectString(), "my-group-id");
        kafkaConsumer.init();
    }

    @After
    public void tearDown() throws IOException {
        kafkaProducer.destroy();
        kafkaConsumer.destroy();
        kafkaSimpleConsumer.close();
        kafkaServer.shutdown();
        zkServer.stop();
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
