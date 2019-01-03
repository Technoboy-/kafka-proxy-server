package com.tt.kafka.push.server.boostrap;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.consumer.ConsumerConfig;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.push.server.consumer.PushServerConsumer;
import com.tt.kafka.push.server.consumer.PushServerMessageListenerService;
import com.tt.kafka.util.Preconditions;

/**
 * @Author: Tboy
 */
public class PushServer {

    private final PushConfigs serverConfigs;

    private final PushServerConsumer consumer;

    private MessageListenerService messageListenerService;

    private PushTcpServer pushTcpServer;

    public PushServer(PushConfigs serverConfigs){
        this.serverConfigs = serverConfigs;
        ConsumerConfig consumerConfigs = new ConsumerConfig(serverConfigs.getServerKafkaServerList(), serverConfigs.getServerTopic(), serverConfigs.getServerGroupId());
        consumerConfigs.setAutoCommit(false);
        this.consumer = new PushServerConsumer(consumerConfigs);
    }

    public void setPushTcpServer(PushTcpServer pushTcpServer){
        this.pushTcpServer = pushTcpServer;
    }

    public void start(){
        Preconditions.checkArgument(this.pushTcpServer != null, "pushTcpServer is null");
        this.messageListenerService = new PushServerMessageListenerService(pushTcpServer, serverConfigs);
        consumer.setMessageListenerService(messageListenerService);
        consumer.start();
    }

    public void close(){
        consumer.close();
        pushTcpServer.close();
    }
}
