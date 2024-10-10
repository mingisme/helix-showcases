package com.envisioniot.enos;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;


@StateModelInfo(initialState = "OFFLINE", states = {
        "OFFLINE", "MASTER", "SLAVE"
})
public class FileStoreStateModel extends StateModel {
    private final HelixManager helixManager;
    private final String resourceName;
    private final String partitionName;
    private final InstanceConfig instanceConfig;
    private final String instanceName;

    private DirectoryWatcher directoryWatcher;

    public FileStoreStateModel(HelixManager helixManager, String resourceName, String partitionName) {
        this.helixManager = helixManager;
        this.resourceName = resourceName;
        this.partitionName = partitionName;
        this.instanceConfig = helixManager.getClusterManagmentTool().getInstanceConfig(helixManager.getClusterName(), helixManager.getInstanceName());
        this.instanceName = helixManager.getInstanceName();
    }

    @Transition(from = "OFFLINE", to = "SLAVE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
        System.out.println(instanceName + " transitioning from " + message.getFromState() + " to " + message.getToState() + " for " + partitionName);


        System.out.println(instanceName + " transitioned from " + message.getFromState() + " to " + message.getToState() + " for " + partitionName);
    }

    @Transition(from = "SLAVE", to = "MASTER")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) throws Exception {
        System.out.println(instanceName + " transitioning from " + message.getFromState() + " to " + message.getToState() + " for " + partitionName);
        directoryWatcher = new DirectoryWatcher(message.getResourceName(), helixManager);
        directoryWatcher.start();

        System.out.println(instanceName + " transitioned from " + message.getFromState() + " to " + message.getToState() + " for " + partitionName);
    }

    @Transition(from = "MASTER", to = "SLAVE")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
        System.out.println(instanceName + " transitioning from " + message.getFromState() + " to " + message.getToState() + " for " + partitionName);

        System.out.println(instanceName + " transitioned from " + message.getFromState() + " to " + message.getToState() + " for " + partitionName);
    }

    @Transition(from = "SLAVE", to = "OFFLINE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
        System.out.println(instanceName + " transitioning from " + message.getFromState() + " to " + message.getToState() + " for " + partitionName);

        System.out.println(instanceName + " transitioned from " + message.getFromState() + " to " + message.getToState() + " for " + partitionName);
    }

    @Override
    public void reset() {
        System.out.println("Default reset() invoked");
    }
}
