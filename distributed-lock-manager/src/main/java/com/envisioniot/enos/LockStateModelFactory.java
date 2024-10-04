package com.envisioniot.enos;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class LockStateModelFactory extends StateModelFactory<LockStateModel> {

    private final String instanceName;

    public LockStateModelFactory(String instanceName){
        this.instanceName = instanceName;
    }

    @Override
    public LockStateModel createNewStateModel(String resourceName, String partitionName) {
        return new LockStateModel(instanceName, partitionName);
    }
}
