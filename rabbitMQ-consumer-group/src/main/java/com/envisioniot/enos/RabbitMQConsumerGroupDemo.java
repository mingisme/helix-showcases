package com.envisioniot.enos;

import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.zkclient.ZkServer;

import java.io.File;
import java.util.Map;
import java.util.TreeSet;

public class RabbitMQConsumerGroupDemo {
    public static void main(String[] args) throws Exception {
        int zkPort = 2199;
        ZkServer zkServer = startLocalZookeeper(zkPort);

        String clusterName = "rabbitmq-consumer-group";
        String zkAddress = "localhost:" + zkPort;

        ZKHelixAdmin admin = new ZKHelixAdmin.Builder().setZkAddress(zkAddress).build();
        admin.addCluster(clusterName, true);
        StateModelDefinition smd = OnlineOfflineSMD.build();

        String stateModelDef = OnlineOfflineSMD.name;
        admin.addStateModelDef(clusterName, stateModelDef, smd, true);

        String resource = MqConsumer.QUEUE_NAME;
        int partitionNumber = 6;
        admin.addResource(clusterName, resource, partitionNumber, stateModelDef, IdealState.RebalanceMode.FULL_AUTO.name());
        admin.rebalance(clusterName, resource, 2);

        String mqServer = "localhost";
        ConsumerInstance[] instances = new ConsumerInstance[3];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = new ConsumerInstance("localhost_1200" + i, zkAddress, clusterName, stateModelDef, mqServer);
        }
        for (ConsumerInstance instance : instances) {
            instance.start();
        }
        Thread.sleep(2000);
        HelixManager controller = HelixControllerMain.startHelixController(zkAddress, clusterName, "controller",
                HelixControllerMain.STANDALONE);
        System.out.println("Started controller");
        Thread.sleep(2000);

        ProducerInstance producer = new ProducerInstance(mqServer, partitionNumber);
        producer.start();

        Thread.sleep(5000);
        printStatus(admin, clusterName, resource);

        instances[0].interrupt();
        Thread.sleep(5000);
        printStatus(admin, clusterName, resource);

        instances[1].interrupt();
        Thread.sleep(5000);
        printStatus(admin, clusterName, resource);

        producer.interrupt();
        Thread.sleep(2000);
        System.out.println("produce: " + MessageCounter.produceCount + ", consume: " + MessageCounter.consumeCount);

        controller.disconnect();
        for (ConsumerInstance instance : instances) {
            instance.interrupt();
        }
        zkServer.shutdown();

        System.exit(0);

    }

    private static void printStatus(ZKHelixAdmin admin, String clusterName, String resource) {
        System.out.println("\n---external view of " + resource + "---");
        ExternalView externalView = admin.getResourceExternalView(clusterName, resource);
        TreeSet<String> treeSet = new TreeSet<String>(externalView.getPartitionSet());
        for (String partition : treeSet) {
            Map<String, String> stateMap = externalView.getStateMap(partition);
            System.out.println("partition: " + partition);
            stateMap.forEach((key, value) -> System.out.println("\t" + key + ":\t" + value));
        }
        System.out.println();
    }


    private static ZkServer startLocalZookeeper(int port) throws Exception {
        String baseDir = "/tmp/demo/";
        final String dataDir = baseDir + "zk/dataDir";
        final String logDir = baseDir + "/tmp/logDir";
        FileUtils.deleteDirectory(new File(dataDir));
        FileUtils.deleteDirectory(new File(logDir));
        ZkServer zkServer = new ZkServer(dataDir, logDir, zk -> {
        }, port);
        zkServer.start();
        return zkServer;
    }
}
