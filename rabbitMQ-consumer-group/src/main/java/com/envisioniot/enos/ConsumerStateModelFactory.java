package com.envisioniot.enos;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class ConsumerStateModelFactory extends StateModelFactory<ConsumerStateModel> {

    private final String instanceName;
    private final String mqServer;

    public ConsumerStateModelFactory(String instanceName, String mqServer){
        this.instanceName = instanceName;
        this.mqServer = mqServer;
    }

    @Override
    public ConsumerStateModel createNewStateModel(String resourceName, String partitionName) {
        return new ConsumerStateModel(instanceName, partitionName, mqServer);
    }
}
