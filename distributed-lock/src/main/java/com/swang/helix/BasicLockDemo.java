package com.swang.helix;

import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.zkclient.ZkServer;

import java.io.File;


public class BasicLockDemo {
    public static void main(String[] args) throws Exception {
        int zkPort = 2199;
        startLocalZookeeper(zkPort);

        int workerNumber = 3;
        BasicWorker[] workers = new BasicWorker[workerNumber];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new BasicWorker("worker" + i, "localhost:" + zkPort);
        }
        for (BasicWorker worker : workers) {
            worker.start();
        }
    }

    private static void startLocalZookeeper(int port) throws Exception {
        String baseDir = "/tmp/demo/";
        final String dataDir = baseDir + "zk/dataDir";
        final String logDir = baseDir + "/tmp/logDir";
        FileUtils.deleteDirectory(new File(dataDir));
        FileUtils.deleteDirectory(new File(logDir));
        new ZkServer(dataDir, logDir, zk -> {
        }, port).start();
    }
}
