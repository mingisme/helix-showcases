package com.envisioniot.enos;

import org.apache.helix.HelixManager;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class FileStoreStateModelFactory extends StateModelFactory<FileStoreStateModel> {

    private final HelixManager helixManager;

    public FileStoreStateModelFactory(HelixManager helixManager) {
        this.helixManager = helixManager;
    }

    @Override
    public FileStoreStateModel createNewStateModel(String resourceName, String partitionName) {
        return new FileStoreStateModel(helixManager, resourceName, partitionName);
    }
}
