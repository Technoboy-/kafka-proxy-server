package com.tt.kafka.push.server.biz;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.client.zookeeper.KafkaZookeeperConfig;
import com.tt.kafka.consumer.ConsumerConfig;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.push.server.biz.registry.RegistryCenter;
import com.tt.kafka.push.server.consumer.AcknowledgeMessageListenerService;
import com.tt.kafka.push.server.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.util.StringUtils;

/**
 * @Author: Tboy
 */
public class PushServer {

    private final DefaultKafkaConsumerImpl consumer;

    private final MessageListenerService messageListenerService;

    private final NettyServer nettyServer;

    public PushServer(){
        PushConfigs serverConfigs = new PushConfigs(true);
        String kafkaServerList = serverConfigs.getServerKafkaServerList();
        if(StringUtils.isBlank(kafkaServerList)){
            kafkaServerList = KafkaZookeeperConfig.getBrokerIds(serverConfigs.getZookeeperServerList(), serverConfigs.getZookeeperNamespace());
        }
        ConsumerConfig consumerConfigs = new ConsumerConfig(kafkaServerList, serverConfigs.getServerTopic(), serverConfigs.getServerGroupId());
        consumerConfigs.setAutoCommit(false);
        this.consumer = new DefaultKafkaConsumerImpl(consumerConfigs);
        this.nettyServer = new NettyServer(consumer);
        this.messageListenerService = new AcknowledgeMessageListenerService();
        this.consumer.setMessageListenerService(messageListenerService);
    }

    public void start(){
        this.nettyServer.start();
        this.consumer.start();
    }

    public void close(){
        this.consumer.close();
        this.nettyServer.close();
    }
}
