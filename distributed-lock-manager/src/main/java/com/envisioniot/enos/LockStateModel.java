package com.envisioniot.enos;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;


@StateModelInfo(initialState = "OFFLINE", states = {"OFFLINE", "ONLINE"})
public class LockStateModel extends StateModel {

    private final String instanceName;
    private final String lockName;

    public LockStateModel(String instanceName, String lockName) {
        this.instanceName = instanceName;
        this.lockName = lockName;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void lock(Message message, NotificationContext context) {
        System.out.println(instanceName + " acquired lock: " + lockName);
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void release(Message message, NotificationContext context) {
        System.out.println(instanceName + " releasing lock: " + lockName);
    }

}
