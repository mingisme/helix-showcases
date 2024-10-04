package com.envisioniot.enos;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class MqConsumer extends Thread {
    public static final String QUEUE_NAME = "test_queue";

    private final String instanceName;
    private final String parition;
    private final String mqServer;


    public MqConsumer(String instanceName, String parition, String mqServer) {
        this.instanceName = instanceName;
        this.parition = parition;
        this.mqServer = mqServer;
    }

    @Override
    public void run() {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(mqServer);
        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();
            String queue = parition;
            channel.queueDeclare(queue, true, false, false, null);
            System.out.println(" [*] " + instanceName + " Waiting for messages on " + queue + ".");
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Rcvd '" + queue + "':'" + message + "'");
                try {
                    MessageCounter.consumeCount.incrementAndGet();
                } finally {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            };
            channel.basicConsume(queue, false, deliverCallback, consumerTag -> {
            });
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                channel.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
