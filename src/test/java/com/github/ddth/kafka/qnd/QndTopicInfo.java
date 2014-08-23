package com.github.ddth.kafka.qnd;

import com.github.ddth.zookeeper.ZooKeeperClient;

public class QndTopicInfo {
    public static void main(String[] args) throws Exception {
        ZooKeeperClient zkClient = new ZooKeeperClient("localhost:2181/kafka");
        zkClient.init();

        Object data = zkClient.getDataJson("/brokers/topics/testtopic1");
        System.out.println(data);

        data = zkClient.getDataJson("/brokers/topics/testtopic");
        System.out.println(data);

        zkClient.destroy();
    }
}
