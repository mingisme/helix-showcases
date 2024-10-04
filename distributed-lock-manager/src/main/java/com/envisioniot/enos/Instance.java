package com.envisioniot.enos;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;

import java.util.List;

public class Instance extends Thread {

    private final String zkAddress;
    private final String clusterName;
    private final String stateModelDef;

    public Instance(String name, String zkAddress, String clusterName, String stateModelDef) {
        super(name);
        this.zkAddress = zkAddress;
        this.clusterName = clusterName;
        this.stateModelDef = stateModelDef;
    }

    @Override
    public void run() {
        System.out.println("Starting " + getName());
        ZKHelixAdmin admin = new ZKHelixAdmin.Builder().setZkAddress(zkAddress).build();
        List<String> instancesInCluster = admin.getInstancesInCluster(clusterName);
        if(instancesInCluster == null || !instancesInCluster.contains(getName())) {
            InstanceConfig config = new InstanceConfig(getName());
            admin.addInstance(clusterName, config);
        }

        HelixManager helixManager = HelixManagerFactory.getZKHelixManager(clusterName, getName(), InstanceType.PARTICIPANT, zkAddress);
        helixManager.getStateMachineEngine().registerStateModelFactory(this.stateModelDef, new LockStateModelFactory(getName()));
        try {
            helixManager.connect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("Started " + getName());
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.err.println(getName() + " crash");
            helixManager.disconnect();
        }

    }
}
