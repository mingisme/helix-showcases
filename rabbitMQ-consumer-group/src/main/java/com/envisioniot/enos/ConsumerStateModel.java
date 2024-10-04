package com.envisioniot.enos;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;


@StateModelInfo(initialState = "OFFLINE", states = {"OFFLINE", "ONLINE"})
public class ConsumerStateModel extends StateModel {

    private final String instanceName;
    private final String partitionName;
    private final String mqServer;

    private MqConsumer consumer = null;

    public ConsumerStateModel(String instanceName, String partitionName, String mqServer) {
        this.instanceName = instanceName;
        this.partitionName = partitionName;
        this.mqServer = mqServer;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void startConsume(Message message, NotificationContext context) {
        System.out.println(instanceName + " assign to: " + partitionName);
        if (consumer == null) {
            System.out.println("Starting mqConsumer for " + partitionName + ", on " + instanceName);
            consumer = new MqConsumer(instanceName, partitionName, mqServer);
            consumer.start();
            System.out.println("Started mqConsumer for " + partitionName + ", on " + instanceName);
        }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void stopConsume(Message message, NotificationContext context) throws InterruptedException {
        System.out.println(instanceName + " release from: " + partitionName);
        if (consumer != null) {
            System.out.println("Stopping mqConsumer for " + partitionName + ", on " + instanceName);
            consumer.interrupt();
            consumer.join(2000);
            consumer = null;
            System.out.println("Stopped mqConsumer for " + partitionName + ", on " + instanceName);
        }
    }


    @Override
    public void reset() {
        System.out.println("Default reset() invoked");
        if (consumer != null) {
            System.out.println("Stopping " + instanceName + " for " + partitionName + "...");

            consumer.interrupt();
            try {
                consumer.join(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            consumer = null;
            System.out.println("Stopping " + instanceName + " for " + partitionName + " done");
        }
    }

}
