package com.envisioniot.enos;

import org.apache.commons.io.FileUtils;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.zkclient.ZkServer;

import java.io.File;

public class RsyncReplicatedFileSystemDemo {
    public static void main(String[] args) throws Exception {
        int zkPort = 2199;
        ZkServer zkServer = startLocalZookeeper(zkPort);

        String clusterName = "file-store-test";
        String zkAddress = "localhost:" + zkPort;

        ZKHelixAdmin admin = new ZKHelixAdmin.Builder().setZkAddress(zkAddress).build();
        admin.addCluster(clusterName, true);

        StateModelDefinition stateModelDefinition = MasterSlaveSMD.build();
        String stateModelDefName = MasterSlaveSMD.name;
        admin.addStateModelDef(clusterName, stateModelDefName, stateModelDefinition, true);

        String resourceName = "repository";
        admin.addResource(clusterName, resourceName, 1, stateModelDefName, IdealState.RebalanceMode.SEMI_AUTO.name());
        admin.rebalance(clusterName, resourceName, 3);

        String baseDir = "/tmp/file-store/";



        zkServer.shutdown();
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
