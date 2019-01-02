package com.tt.kafka.push.server.boostrap;

import com.tt.kafka.consumer.ConsumerConfig;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.push.server.PushServerConfigs;
import com.tt.kafka.push.server.consumer.PushServerKafkaConsumer;
import com.tt.kafka.push.server.consumer.PushServerMessageListenerService;
import com.tt.kafka.push.server.netty.PushTcpServer;
import com.tt.kafka.push.server.service.RegisterMetadata;
import com.tt.kafka.push.server.service.RegisterService;
import com.tt.kafka.push.server.zookeeper.ZookeeperClient;
import com.tt.kafka.util.NetUtils;

/**
 * @Author: Tboy
 */
public class Bootstrap {

    public static void main(String[] args) {
        //
        PushServerConfigs serverConfigs = new PushServerConfigs();

        //
        ZookeeperClient zookeeperClient = new ZookeeperClient(serverConfigs);
        zookeeperClient.start();
        //
        RegisterService registerService = new RegisterService(zookeeperClient);
        RegisterMetadata.Address address = new RegisterMetadata.Address(NetUtils.getLocalIp(), serverConfigs.getPort());
        RegisterMetadata registerMetadata = new RegisterMetadata(serverConfigs.getZookeeperTopic(), address);
        registerService.register(registerMetadata);
        //
        PushTcpServer pushTcpServer = new PushTcpServer(serverConfigs);
        pushTcpServer.start();
        //
        ConsumerConfig consumerConfigs = new ConsumerConfig(serverConfigs.getKafkaServerList(), serverConfigs.getZookeeperTopic(), serverConfigs.getGroupId());
        consumerConfigs.setAutoCommit(false);
        PushServerKafkaConsumer consumer = new PushServerKafkaConsumer(consumerConfigs);
        MessageListenerService messageListenerService = new PushServerMessageListenerService(pushTcpServer, serverConfigs);
        consumer.setMessageListenerService(messageListenerService);
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            pushTcpServer.close();
            zookeeperClient.close();
        }));
    }

}
