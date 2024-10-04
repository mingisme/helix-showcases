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

public class DistributedLockManagerDemo {
    public static void main(String[] args) throws Exception {

        int zkPort = 2199;
        ZkServer zkServer = startLocalZookeeper(zkPort);

        String clusterName = "distributed-lock-manager";
        String zkAddress = "localhost:" + zkPort;

        ZKHelixAdmin admin = new ZKHelixAdmin.Builder().setZkAddress(zkAddress).build();
        admin.addCluster(clusterName, true);
        StateModelDefinition smd = OnlineOfflineSMD.build();

        String stateModelDef = OnlineOfflineSMD.name;
        admin.addStateModelDef(clusterName, stateModelDef, smd, true);

        String lockGroupName = "lock-group";
        admin.addResource(clusterName, lockGroupName, 12, stateModelDef, IdealState.RebalanceMode.FULL_AUTO.name());
        admin.rebalance(clusterName, lockGroupName, 1);

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
        printStatus(admin, clusterName, lockGroupName);

        instances[0].interrupt();

        Thread.sleep(1000);
        printStatus(admin, clusterName, lockGroupName);

        instances[0] = new Instance("localhost_12000", zkAddress, clusterName, stateModelDef);
        instances[0].start();
        Thread.sleep(1000);
        printStatus(admin, clusterName, lockGroupName);

        controller.disconnect();
        for(Instance instance : instances){
            instance.interrupt();
        }
        zkServer.shutdown();
    }

    private static void printStatus(ZKHelixAdmin admin, String clusterName, String lockGroupName) {
        ExternalView externalView = admin.getResourceExternalView(clusterName, lockGroupName);
        // System.out.println(externalView);
        TreeSet<String> treeSet = new TreeSet<String>(externalView.getPartitionSet());
        System.out.println("lockName" + "\t" + "acquired By");
        System.out.println("======================================");
        for (String lockName : treeSet) {
            Map<String, String> stateMap = externalView.getStateMap(lockName);
            String acquiredBy = stateMap.entrySet().stream().filter(e -> "ONLINE".equals(e.getValue())).map(Map.Entry::getKey).findFirst().orElse("NONE");
            System.out.println(lockName + "\t" +  acquiredBy);
        }
    }

    private static ZkServer startLocalZookeeper(int port) throws Exception {
        String baseDir = "/tmp/basicLockDemo/";
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
