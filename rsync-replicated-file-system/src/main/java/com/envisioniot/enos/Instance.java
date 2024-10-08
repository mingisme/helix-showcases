package com.envisioniot.enos;

import org.apache.commons.io.FileUtils;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Instance extends Thread {

    private final String instanceName;
    private final String clusterName;
    private final String zkAddress;
    private final String baseDir;

    public Instance(String clusterName, String instanceName, String zkAddress, String baseDir) {
        this.instanceName = instanceName;
        this.clusterName = clusterName;
        this.zkAddress = zkAddress;
        this.baseDir = baseDir;
    }

    @Override
    public void run() {
        System.out.println("Start " + instanceName);
        ZKHelixAdmin admin = new ZKHelixAdmin.Builder().setZkAddress(zkAddress).build();
        List<String> instancesInCluster = admin.getInstancesInCluster(clusterName);
        if (instancesInCluster == null || !instancesInCluster.contains(instanceName)) {
            InstanceConfig config = InstanceConfig.toInstanceConfig(instanceName);
            admin.addInstance(clusterName, config);
        }
        addConfiguration(admin, baseDir, clusterName, instanceName);

        System.out.println("Started " + instanceName);
    }

    private static void addConfiguration(ZKHelixAdmin admin, String baseDir, String clusterName,
                                         String instanceName) {
        Map<String, String> properties = new HashMap<>();
        HelixConfigScopeBuilder builder = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT);
        HelixConfigScope instanceScope =
                builder.forCluster(clusterName).forParticipant(instanceName).build();
        properties.put("change_log_dir", baseDir + instanceName + "/translog");
        properties.put("file_store_dir", baseDir + instanceName + "/filestore");
        properties.put("check_point_dir", baseDir + instanceName + "/checkpoint");
        admin.setConfig(instanceScope, properties);
        try {
            FileUtils.deleteDirectory(new File(properties.get("change_log_dir")));
            FileUtils.deleteDirectory(new File(properties.get("file_store_dir")));
            FileUtils.deleteDirectory(new File(properties.get("check_point_dir")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        new File(properties.get("change_log_dir")).mkdirs();
        new File(properties.get("file_store_dir")).mkdirs();
        new File(properties.get("check_point_dir")).mkdirs();
    }
}
