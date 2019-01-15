package com.owl.kafka.push.server.biz;

import com.owl.kafka.client.zookeeper.KafkaZookeeperConfig;
import com.owl.kafka.consumer.ConsumerConfig;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.push.server.biz.service.DLQService;
import com.owl.kafka.push.server.biz.service.InstanceHolder;
import com.owl.kafka.push.server.consumer.AcknowledgeMessageListenerService;
import com.owl.kafka.push.server.consumer.PushServerConsumer;
import com.owl.kafka.util.StringUtils;

/**
 * @Author: Tboy
 */
public class PushServer {

    private final PushServerConsumer consumer;

    private final MessageListenerService messageListenerService;

    private final NettyServer nettyServer;

    private final DLQService dlqService;

    public PushServer(){
        String kafkaServerList = ServerConfigs.I.getServerKafkaServerList();
        if(StringUtils.isBlank(kafkaServerList)){
            kafkaServerList = KafkaZookeeperConfig.getBrokerIds(ServerConfigs.I.getZookeeperServerList(), ServerConfigs.I.getZookeeperNamespace());
        }
        ConsumerConfig consumerConfigs = new ConsumerConfig(kafkaServerList, ServerConfigs.I.getServerTopic(), ServerConfigs.I.getServerGroupId());
        consumerConfigs.setAutoCommit(false);
        this.consumer = new PushServerConsumer(consumerConfigs);
        this.nettyServer = new NettyServer(consumer);
        this.messageListenerService = new AcknowledgeMessageListenerService();
        this.consumer.setMessageListenerService(messageListenerService);
        //
        this.dlqService = new DLQService(kafkaServerList, ServerConfigs.I.getServerTopic(), ServerConfigs.I.getServerGroupId(), ServerConfigs.I.getZookeeperServerList());
        InstanceHolder.I.setDLQService(this.dlqService);
    }

    public void start(){
        this.nettyServer.start();
        this.consumer.start();
    }

    public void close(){
        this.consumer.close();
        this.nettyServer.close();
        this.dlqService.close();
    }
}
