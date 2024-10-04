package com.envisioniot.enos;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class ElectionStateModelFactory extends StateModelFactory<ElectionStateModel> {

    private final String instanceName;

    public ElectionStateModelFactory(String instanceName){
        this.instanceName = instanceName;
    }

    @Override
    public ElectionStateModel createNewStateModel(String resourceName, String partitionName) {
        return new ElectionStateModel(instanceName);
    }
}
