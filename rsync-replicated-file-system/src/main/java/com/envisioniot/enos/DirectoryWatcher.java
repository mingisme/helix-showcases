package com.envisioniot.enos;

import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.zookeeper.data.Stat;

public class DirectoryWatcher {

    private final String resource;
    private final HelixManager helixManager;


    private final InstanceConfig instanceConfig;
    private final ZkHelixPropertyStore<ZNRecord> helixPropertyStore;
    private FileAlterationMonitor monitor;

    public DirectoryWatcher(String resource, HelixManager helixManager) {
        this.resource = resource;
        this.helixManager = helixManager;
        String clusterName = helixManager.getClusterName();
        String instanceName = helixManager.getInstanceName();
        instanceConfig = helixManager.getClusterManagmentTool().getInstanceConfig(clusterName, instanceName);
        helixPropertyStore = helixManager.getHelixPropertyStore();
    }

    public void start() throws Exception {
        String checkpointDirPath = instanceConfig.getRecord().getSimpleField("check_point_dir");
        CheckpointFile checkpointFile = new CheckpointFile(checkpointDirPath);
        ChangeRecord lastRecordProcessed = checkpointFile.getCheckpoint();
        DataUpdater<ZNRecord> updater = new HighWaterMarkUpdater(resource, lastRecordProcessed);
        helixPropertyStore.update("/TRANSACTION_ID_METADATA/" + resource, updater, AccessOption.PERSISTENT);
        Stat stat = new Stat();
        ZNRecord znRecord = helixPropertyStore.get("/TRANSACTION_ID_METADATA/" + resource, stat, AccessOption.PERSISTENT);
        int startGen = Integer.parseInt(znRecord.getSimpleField("currentGen"));
        int startSeq = Integer.parseInt(znRecord.getSimpleField("currentGenStartSeq"));
        String changeLogDir = instanceConfig.getRecord().getSimpleField("change_log_dir");

        String fileStoreDir = instanceConfig.getRecord().getSimpleField("file_store_dir");

        FileAlterationObserver observer = new FileAlterationObserver(fileStoreDir);
        observer.addListener(new ChangeLogGenerator(changeLogDir, startGen, startSeq));
        long pollingInterval = 2000;
        monitor = new FileAlterationMonitor(pollingInterval);
        monitor.addObserver(observer);
        monitor.start();
    }

    public void stop() throws Exception {
        if (monitor != null) {
            monitor.stop();
        }
    }

    private final class HighWaterMarkUpdater implements DataUpdater<ZNRecord> {

        private final String resourceName;
        private final ChangeRecord lastChangeRecord;

        private HighWaterMarkUpdater(String resourceName, ChangeRecord lastChangeRecord) {
            this.resourceName = resourceName;
            this.lastChangeRecord = lastChangeRecord;
        }

        @Override
        public ZNRecord update(ZNRecord currentData) {
            ZNRecord newRec = new ZNRecord(resourceName);

            if (currentData != null) {
                int currentGen = convertToInt(currentData.getSimpleField("currentGen"), 0);
                newRec.setSimpleField("currentGen", Integer.toString(currentGen + 1));
                if (currentGen > 0) {
                    newRec.setSimpleField("prevGen", Integer.toString(currentGen));
                    long localEndSeq = 1;
                    if (lastChangeRecord != null) {
                        localEndSeq = lastChangeRecord.txid;
                    }
                    newRec.setSimpleField("prevGenEndSeq", Long.toString(localEndSeq));
                }
            } else {
                newRec.setSimpleField("currentGen", Integer.toString(1));

            }
            newRec.setSimpleField("currentGenStartSeq", Integer.toString(1));

            return newRec;
        }

        private int convertToInt(String number, int defaultValue) {
            try {
                return Integer.parseInt(number);
            } catch (Exception e) {
                return defaultValue;
            }
        }
    }
}
