package com.envisioniot.enos;

import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.*;
import org.apache.helix.zookeeper.zkclient.ZkServer;

import java.io.File;
import java.util.Map;
import java.util.TreeSet;

public class LeaderElectionDemo {
    public static void main(String[] args) throws Exception {

        int zkPort = 2199;
        ZkServer zkServer = startLocalZookeeper(zkPort);

        String clusterName = "instance-leader-election";
        String zkAddress = "localhost:" + zkPort;

        ZKHelixAdmin admin = new ZKHelixAdmin.Builder().setZkAddress(zkAddress).build();
        admin.addCluster(clusterName, true);
        StateModelDefinition smd = LeaderStandbySMD.build();

        String stateModelDef = LeaderStandbySMD.name;
        admin.addStateModelDef(clusterName, stateModelDef, smd, true);

        String resource = "myService";
        admin.addResource(clusterName, resource, 1, stateModelDef, IdealState.RebalanceMode.FULL_AUTO.name());
        admin.rebalance(clusterName, resource, 4);

        Instance[] instances = new Instance[3];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = new Instance("localhost_1200" + i, zkAddress, clusterName, stateModelDef);
        }
        for (Instance instance : instances) {
            instance.start();
        }

        Thread.sleep(1000);
        HelixManager controller = HelixControllerMain.startHelixController(zkAddress, clusterName, "controller",
                HelixControllerMain.STANDALONE);
        System.out.println("Started controller");

        Thread.sleep(1000);
        printStatus(admin, clusterName, resource);

        instances[0].interrupt();
        Thread.sleep(1000);
        printStatus(admin, clusterName, resource);

        instances[1].interrupt();
        Thread.sleep(1000);
        printStatus(admin, clusterName, resource);

        instances[0] = new Instance("localhost_12000", zkAddress, clusterName, stateModelDef);
        instances[0].start();
        Thread.sleep(1000);
        printStatus(admin, clusterName, resource);

        controller.disconnect();
        for (Instance instance : instances) {
            instance.interrupt();
        }
        zkServer.shutdown();

    }

    private static void printStatus(ZKHelixAdmin admin, String clusterName, String resource) {
        System.out.println("\n---external view of " + resource + "---");
        ExternalView externalView = admin.getResourceExternalView(clusterName, resource);
        TreeSet<String> treeSet = new TreeSet<String>(externalView.getPartitionSet());
        for (String partition : treeSet) {
            Map<String, String> stateMap = externalView.getStateMap(partition);
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
