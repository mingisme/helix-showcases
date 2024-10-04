package com.envisioniot.enos;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.nio.charset.StandardCharsets;

import static com.envisioniot.enos.MqConsumer.QUEUE_NAME;

public class ProducerInstance extends Thread {
    public String QUEUE_NAME_PREFIX = QUEUE_NAME + "_";

    private final String mqServer;
    private final int partitionNumber;

    public ProducerInstance(String mqServer, int partitionNumber) {
        this.mqServer = mqServer;
        this.partitionNumber = partitionNumber;
    }


    public void run() {

        int count = Integer.MAX_VALUE;
        System.out.println("Sending " + count + " messages with random topic id");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(mqServer);

        try (Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();
            for (int i = 0; i < partitionNumber; i++) {
                channel.queueDeclare(QUEUE_NAME_PREFIX + i, true, false, false, null);
            }

            for (int i = 0; i < count; i++) {
                int rand = i % partitionNumber;
                String queueName = QUEUE_NAME_PREFIX + rand;
                String message = "" + i;
                channel.basicPublish("", queueName,
                        MessageProperties.MINIMAL_BASIC,
                        message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + queueName + "':'" + message + "'");
                MessageCounter.produceCount.incrementAndGet();

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    System.err.println("Producer crash");
                    throw e;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
