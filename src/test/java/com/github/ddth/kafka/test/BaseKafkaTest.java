//package com.github.ddth.kafka.test;
//
//import java.io.File;
//import java.io.IOException;
//import java.net.InetSocketAddress;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//
//import org.apache.kafka.common.security.JaasUtils;
//import org.apache.zookeeper.server.NIOServerCnxnFactory;
//import org.apache.zookeeper.server.ServerCnxnFactory;
//import org.apache.zookeeper.server.ZooKeeperServer;
//import org.junit.After;
//import org.junit.Before;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.github.ddth.kafka.KafkaClient;
//import com.github.ddth.kafka.KafkaClient.ProducerType;
//import com.github.ddth.kafka.KafkaMessage;
//
//import junit.framework.TestCase;
//import kafka.admin.TopicCommand;
//import kafka.server.KafkaConfig;
//import kafka.server.KafkaServerStartable;
//import kafka.utils.ZkUtils;
//
///**
// * Unit test for simple App.
// */
//public abstract class BaseKafkaTest extends TestCase {
//    /**
//     * Create the test case
//     *
//     * @param testName
//     *            name of the test case
//     */
//    public BaseKafkaTest(String testName) {
//        super(testName);
//    }
//
//    private final static Logger LOGGER = LoggerFactory.getLogger(BaseKafkaTest.class);
//
//    private static ServerCnxnFactory startZkServer(int zkPort) {
//        File snapshotDir;
//        File logDir;
//        try {
//            snapshotDir = java.nio.file.Files.createTempDirectory("zookeeper-snapshot").toFile();
//            logDir = java.nio.file.Files.createTempDirectory("zookeeper-logs").toFile();
//        } catch (IOException e) {
//            throw new RuntimeException("Unable to create Zookeeper temp dirs!", e);
//        }
//
//        LOGGER.info("Zookeeper snapshot dir: " + snapshotDir.getAbsolutePath());
//        snapshotDir.deleteOnExit();
//
//        LOGGER.info("Zookeeper log dir: " + logDir.getAbsolutePath());
//        logDir.deleteOnExit();
//
//        try {
//            int tickTime = 500;
//            ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
//            ServerCnxnFactory zkFactory = NIOServerCnxnFactory.createFactory();
//            zkFactory.configure(new InetSocketAddress("127.0.0.1", zkPort), 16);
//            zkFactory.startup(zkServer);
//            return zkFactory;
//        } catch (Exception e) {
//            throw new RuntimeException("Unable to start ZooKeeper!", e);
//        }
//    }
//
//    private static KafkaServerStartable startKafkaServer(int kafkaPort, int zkPort) {
//        File logDir;
//        try {
//            logDir = java.nio.file.Files.createTempDirectory("kafka-logs").toFile();
//        } catch (IOException e) {
//            throw new RuntimeException("Unable to create Kafka temp dirs!", e);
//        }
//
//        LOGGER.info("Kafka log dir: " + logDir.getAbsolutePath());
//        logDir.deleteOnExit();
//
//        Properties kafkaBrokerConfig = new Properties();
//        kafkaBrokerConfig.setProperty("zookeeper.connect", "127.0.0.1:" + zkPort);
//        kafkaBrokerConfig.setProperty("broker.id", "0");
//        kafkaBrokerConfig.setProperty("host.name", "127.0.0.1");
//        kafkaBrokerConfig.setProperty("port", Integer.toString(kafkaPort));
//        kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
//        kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));
//
//        KafkaServerStartable broker = new KafkaServerStartable(new KafkaConfig(kafkaBrokerConfig));
//        broker.startup();
//        return broker;
//    }
//
//    private static KafkaClient createKafkaClient(int kafkaPort) throws Exception {
//        KafkaClient kafkaClient = new KafkaClient("127.0.0.1:" + kafkaPort);
//        kafkaClient.init();
//        return kafkaClient;
//    }
//
//    private static void createTopic(String topicName, int numPartitions, int zkPort) {
//        // setup
//        String[] arguments = new String[9];
//        arguments[0] = "--create";
//        arguments[1] = "--zookeeper";
//        arguments[2] = "127.0.0.1:" + zkPort;
//        arguments[3] = "--replication-factor";
//        arguments[4] = "1";
//        arguments[5] = "--partitions";
//        arguments[6] = "" + Integer.valueOf(numPartitions);
//        arguments[7] = "--topic";
//        arguments[8] = topicName;
//        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);
//
//        ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()), 30000, 30000,
//                JaasUtils.isZkSecurityEnabled());
//        TopicCommand.createTopic(zkUtils, opts);
//    }
//
//    private final static int ZK_PORT = 12181;
//    private final static int KAFKA_PORT = 19092;
//
//    protected KafkaClient kafkaClient;
//    protected ServerCnxnFactory zkFactory;
//    protected KafkaServerStartable kafkaBroker;
//
//    protected void createTopic(String topicName) throws Exception {
//        createTopic(topicName, 1, ZK_PORT);
//    }
//
//    protected void createTopic(String topicName, int numPartitions) throws Exception {
//        createTopic(topicName, numPartitions, ZK_PORT);
//    }
//
//    protected void warnup(String topic, String groupId) throws Exception {
//        kafkaClient.sendMessage(ProducerType.ALL_ACKS, new KafkaMessage(topic, "warnup"));
//        KafkaMessage msg = kafkaClient.consumeMessage(groupId, true, topic, 1000,
//                TimeUnit.MILLISECONDS);
//        while (msg == null) {
//            msg = kafkaClient.consumeMessage(groupId, true, topic, 1000, TimeUnit.MILLISECONDS);
//        }
//    }
//
//    @Before
//    public void setUp() throws Exception {
//        zkFactory = startZkServer(ZK_PORT);
//        kafkaBroker = startKafkaServer(KAFKA_PORT, ZK_PORT);
//        kafkaClient = createKafkaClient(KAFKA_PORT);
//    }
//
//    @After
//    public void tearDown() throws IOException {
//        try {
//            kafkaClient.destroy();
//        } catch (Exception e) {
//        }
//
//        try {
//            kafkaBroker.shutdown();
//        } catch (Exception e) {
//        }
//
//        try {
//            zkFactory.shutdown();
//        } catch (Exception e) {
//        }
//    }
//}
