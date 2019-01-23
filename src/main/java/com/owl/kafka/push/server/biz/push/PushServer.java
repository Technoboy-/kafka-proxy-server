package com.owl.kafka.push.server.biz.push;

import com.owl.kafka.proxy.zookeeper.KafkaZookeeperConfig;
import com.owl.kafka.consumer.ConsumerConfig;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.push.server.biz.NettyServer;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.push.server.biz.registry.RegistryCenter;
import com.owl.kafka.push.server.biz.service.DLQService;
import com.owl.kafka.push.server.consumer.AcknowledgeMessageListenerPushService;
import com.owl.kafka.push.server.consumer.ProxyConsumer;
import com.owl.kafka.util.StringUtils;

/**
 * @Author: Tboy
 */
public class PushServer {

    private final ProxyConsumer consumer;

    private final MessageListenerService messageListenerService;

    private final NettyServer nettyServer;

    private final DLQService dlqService;

    private final PushCenter pushCenter;

    private final RegistryCenter registryCenter;

    public PushServer(){
        String kafkaServerList = ServerConfigs.I.getServerKafkaServerList();
        if(StringUtils.isBlank(kafkaServerList)){
            kafkaServerList = KafkaZookeeperConfig.getBrokerIds(ServerConfigs.I.getZookeeperServerList(), ServerConfigs.I.getZookeeperNamespace());
        }
        this.registryCenter = new RegistryCenter();
        //
        ConsumerConfig consumerConfigs = new ConsumerConfig(kafkaServerList, ServerConfigs.I.getServerTopic(), ServerConfigs.I.getServerGroupId());
        consumerConfigs.setAutoCommit(false);
        this.consumer = new ProxyConsumer(consumerConfigs);
        this.nettyServer = new NettyServer(consumer);

        this.pushCenter = new PushCenter();

        this.messageListenerService = new AcknowledgeMessageListenerPushService(pushCenter);
        this.consumer.setMessageListenerService(messageListenerService);

        this.dlqService = new DLQService(kafkaServerList, ServerConfigs.I.getServerTopic(), ServerConfigs.I.getServerGroupId());
    }

    public void start(){
        this.pushCenter.start();
        this.nettyServer.start();
        this.consumer.start();
    }

    public void close(){
        this.consumer.close();
        this.nettyServer.close();
        this.dlqService.close();
        this.registryCenter.close();
    }
}
