package com.owl.kafka.push.server.consumer;

import com.owl.kafka.client.service.RegisterMetadata;
import com.owl.kafka.client.transport.Address;
import com.owl.kafka.client.zookeeper.ZookeeperClient;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.push.server.biz.registry.RegistryCenter;
import com.owl.kafka.push.server.biz.service.InstanceHolder;
import com.owl.kafka.push.server.biz.service.LeaderElectionService;
import com.owl.kafka.util.NetUtils;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class DLQConsumer implements LeaderLatchListener, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLQConsumer.class);

    private final LeaderElectionService leaderElectionService;

    private Consumer<byte[], byte[]> consumer;

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;

    private final Thread supervisor;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public DLQConsumer(String bootstrapServers, String topic, String groupId){
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        //
        this.supervisor = new Thread(this, "dlq-consumer-selector");
        this.supervisor.setDaemon(true);
        //TODO PATH
        this.leaderElectionService = new LeaderElectionService(InstanceHolder.I.getZookeeperClient().getClient(), "", this);
        this.isRunning.compareAndSet(false, true);
        this.supervisor.start();
    }


    @Override
    public void isLeader() {
        RegisterMetadata metadata = new RegisterMetadata();
        Address address = new Address(NetUtils.getLocalIp(), ServerConfigs.I.getServerPort());
        metadata.setPath(String.format(ServerConfigs.I.ZOOKEEPER_CONSUMERS, this.topic));
        metadata.setAddress(address);
        RegistryCenter.I.getServerRegistry().register(metadata);
        //
        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put("bootstrap.servers", bootstrapServers);
        consumerConfigs.put("group.id", groupId);
        consumerConfigs.put("fetch.max.bytes", 10 * 1024 * 1024); //10m for a request, only fetch little record
        consumerConfigs.put("enable.auto.commit", true);
        consumerConfigs.put("partition.assignment.strategy", "com.owl.kafka.consumer.assignor.CheckTopicStickyAssignor");
        this.consumer = new KafkaConsumer(consumerConfigs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        this.consumer.subscribe(Arrays.asList(topic));
    }

    public boolean hasLeadership() {
        return this.consumer != null && leaderElectionService.hasLeadership();
    }

    public ConsumerRecord<byte[], byte[]> seek(long offset){
        if(isRunning.get() && hasLeadership()){
            TopicPartition topicPartition = new TopicPartition(this.topic, 0);
            consumer.seek(topicPartition, offset - 1);
            ConsumerRecords<byte[], byte[]> records;
            while((records = consumer.poll(0)) != null){
                Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
                while(iterator.hasNext()){
                    ConsumerRecord<byte[], byte[]> next = iterator.next();
                    if(next.offset() == offset){
                        return next;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void notLeader() {
        RegisterMetadata metadata = new RegisterMetadata();
        Address address = new Address(NetUtils.getLocalIp(), ServerConfigs.I.getServerPort());
        metadata.setPath(String.format(ServerConfigs.I.ZOOKEEPER_CONSUMERS, this.topic));
        metadata.setAddress(address);
        RegistryCenter.I.getServerRegistry().unregister(metadata);
        //
        if(this.consumer != null){
            this.consumer.close();
        }
    }

    public void close(){
        this.isRunning.compareAndSet(true, false);
        this.leaderElectionService.close();
    }

    @Override
    public void run() {
        while(isRunning.get()){
            try {
                this.leaderElectionService.select();
            } catch (Exception ex) {
                LOGGER.error("select error", ex);
            }
        }
    }
}
