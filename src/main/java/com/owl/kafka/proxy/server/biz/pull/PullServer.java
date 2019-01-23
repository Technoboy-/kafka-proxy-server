package com.owl.kafka.proxy.server.biz.pull;

import com.owl.kafka.client.consumer.ConsumerConfig;
import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.proxy.zookeeper.KafkaZookeeperConfig;
import com.owl.kafka.client.util.StringUtils;
import com.owl.kafka.proxy.server.biz.NettyServer;
import com.owl.kafka.proxy.server.biz.bo.ServerConfigs;
import com.owl.kafka.proxy.server.biz.registry.RegistryCenter;
import com.owl.kafka.proxy.server.biz.service.DLQService;
import com.owl.kafka.proxy.server.consumer.AcknowledgeMessageListenerPullService;
import com.owl.kafka.proxy.server.consumer.ProxyConsumer;


/**
 * @Author: Tboy
 */
public class PullServer {

    private final ProxyConsumer consumer;

    private final MessageListenerService messageListenerService;

    private final NettyServer nettyServer;

    private final DLQService dlqService;

    private final RegistryCenter registryCenter;

    public PullServer(){
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

        this.messageListenerService = new AcknowledgeMessageListenerPullService();
        this.consumer.setMessageListenerService(messageListenerService);

        this.dlqService = new DLQService(kafkaServerList, ServerConfigs.I.getServerTopic(), ServerConfigs.I.getServerGroupId());
    }

    public void start(){
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
