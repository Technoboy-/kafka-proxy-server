package com.owl.kafka.push.server.biz;

import com.owl.kafka.client.zookeeper.KafkaZookeeperConfig;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.util.StringUtils;
import com.owl.kafka.consumer.ConsumerConfig;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.push.server.consumer.AcknowledgeMessageListenerService;
import com.owl.kafka.push.server.consumer.DefaultKafkaConsumerImpl;

/**
 * @Author: Tboy
 */
public class PushServer {

    private final DefaultKafkaConsumerImpl consumer;

    private final MessageListenerService messageListenerService;

    private final NettyServer nettyServer;

    public PushServer(){
        String kafkaServerList = ServerConfigs.I.getServerKafkaServerList();
        if(StringUtils.isBlank(kafkaServerList)){
            kafkaServerList = KafkaZookeeperConfig.getBrokerIds(ServerConfigs.I.getZookeeperServerList(), ServerConfigs.I.getZookeeperNamespace());
        }
        ConsumerConfig consumerConfigs = new ConsumerConfig(kafkaServerList, ServerConfigs.I.getServerTopic(), ServerConfigs.I.getServerGroupId());
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
