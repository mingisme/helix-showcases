package com.envisioniot.enos;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;


@StateModelInfo(initialState = "OFFLINE", states = {"OFFLINE", "STANDBY", "LEADER"})
public class ElectionStateModel extends StateModel {

    private final String instanceName;

    public ElectionStateModel(String instanceName) {
        this.instanceName = instanceName;
    }

    @Transition(from = "OFFLINE", to = "STANDBY")
    public void onOfflineToStandby(Message message, NotificationContext context) {
        System.out.println(instanceName + " becomes standby from offline");
    }

    @Transition(from = "STANDBY", to = "OFFLINE")
    public void onStandbyToOffline(Message message, NotificationContext context) {
        System.out.println(instanceName + " becomes standby from offline");
    }

    @Transition(from = "STANDBY", to = "LEADER")
    public void onStandbyToLeader(Message message, NotificationContext context) {
        System.out.println(instanceName + " becomes leader from standby");
    }

    @Transition(from = "LEADER", to = "STANDBY")
    public void onLeaderToStandby(Message message, NotificationContext context) {
        System.out.println(instanceName + " becomes standby from leader");
    }

}
