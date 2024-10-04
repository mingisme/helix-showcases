package com.swang.helix;

import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.zkclient.ZkServer;

import java.io.File;


public class PriorityLockDemo {
    public static void main(String[] args) throws Exception {
        int zkPort = 2199;
        ZkServer zkServer = startLocalZookeeper(zkPort);

        int workerNumber = 3;
        PriorityWorker[] workers = new PriorityWorker[workerNumber];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new PriorityWorker("worker" + i, "localhost:" + zkPort, i);
        }
        for (PriorityWorker worker : workers) {
            worker.start();
        }
        zkServer.shutdown();
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
