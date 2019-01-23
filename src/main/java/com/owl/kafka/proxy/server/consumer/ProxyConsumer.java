package com.owl.kafka.proxy.server.consumer;

import com.owl.kafka.client.consumer.ConsumerConfig;
import com.owl.kafka.client.consumer.exceptions.TopicNotExistException;
import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.metric.MonitorImpl;
import com.owl.kafka.client.proxy.zookeeper.KafkaZookeeperConfig;
import com.owl.kafka.client.util.CollectionUtils;
import com.owl.kafka.client.util.Preconditions;
import com.owl.kafka.client.util.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
@SuppressWarnings("all")
public class ProxyConsumer<K, V> implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(ProxyConsumer.class);

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final Consumer<byte[], byte[]> consumer;

    private final Thread worker = new Thread(this, "consumer-poll-worker");

    private final ConsumerConfig configs;

    private MessageListenerService messageListenerService;

    public ProxyConsumer(ConsumerConfig configs) {
        this.configs = configs;

        // KAFKA 0.11 later version.
        if(configs.get("partition.assignment.strategy") == null){
            configs.put("partition.assignment.strategy", "com.owl.kafka.client.consumer.assignor.CheckTopicStickyAssignor");
        }
        String bootstrapServers = configs.getKafkaServers();
        if(StringUtils.isBlank(bootstrapServers)){
            bootstrapServers = KafkaZookeeperConfig.getBrokerIds(configs.getZookeeperServers(), configs.getZookeeperNamespace());
        }
        configs.put("bootstrap.servers", bootstrapServers);
        configs.put("group.id", configs.getGroupId());

        this.consumer = new KafkaConsumer(configs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    public void setMessageListenerService(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
    }

    public void start() {

        Preconditions.checkArgument(CollectionUtils.isEmpty(configs.getTopicPartitions()), "topicPartition is not support in push server");
        Preconditions.checkArgument(messageListenerService != null, "MessageListenerService is null");

        if (start.compareAndSet(false, true)) {
            consumer.subscribe(Arrays.asList(configs.getTopic()), (ConsumerRebalanceListener) messageListenerService);
            //
            worker.setDaemon(true);
            worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Uncaught exceptions in " + worker.getName() + ": ", e);
                }
            });
            worker.start();
            //
            Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));

            LOG.info("kafka consumer startup with info : {}", startupInfo());
        }
    }

    @Override
    public void run() {
        LOG.info(worker.getName() + " start.");
        while (start.get()) {
            long now = System.currentTimeMillis();
            ConsumerRecords<byte[], byte[]> records = null;
            try {
                synchronized (consumer) {
                    records = consumer.poll(configs.getPollTimeout());
                }
            } catch (TopicNotExistException ex){
                StringBuilder builder = new StringBuilder(150);
                builder.append("topic not exist, will close the consumer instance in case of the scenario : ");
                builder.append("using the same groupId for subscribe more than one topic, and one of the topic does not create in the broker, ");
                builder.append("so it will cause the other one consumer in rebalance status for at least 5 minutes due to the kafka inner config.");
                builder.append("To avoid this problem, close the consumer will speed up the rebalancing time");
                LOG.error(builder.toString(), ex);
                close();
            }
            MonitorImpl.getDefault().recordConsumePollTime(System.currentTimeMillis() - now);
            MonitorImpl.getDefault().recordConsumePollCount(1);

            if (LOG.isTraceEnabled() && records != null && !records.isEmpty()) {
                LOG.trace("Received: " + records.count() + " records");
            }
            if (records != null && !records.isEmpty()) {
                invokeMessageService(records);
            }
        }
        LOG.info(worker.getName() + " stop.");
    }

    public void commit(Map<TopicPartition, OffsetAndMetadata> highestOffsetRecords) {
        synchronized (consumer) {
            consumer.commitAsync(highestOffsetRecords, new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if(exception != null){
                        LOG.warn("commit async fail, metadata {}, exceptions {}", offsets, exception);
                    }
                }
            });
            if(LOG.isDebugEnabled()){
                LOG.debug("commit offset : {}", highestOffsetRecords);
            }
        }
    }

    private void invokeMessageService(ConsumerRecords<byte[], byte[]> records) {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
        while (iterator.hasNext()) {
            ConsumerRecord<byte[], byte[]> record = iterator.next();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Processing " + record);
            }
            MonitorImpl.getDefault().recordConsumeRecvCount(1);
            try {
                messageListenerService.onMessage(record);
            } catch (Throwable ex) {
                LOG.error("onMessage error", ex);
                close();
            }
        }
    }

    public ConsumerConfig getConfigs() {
        return configs;
    }

    public void close() {
        LOG.info("KafkaConsumer closing.");
        if(start.compareAndSet(true, false)){
            synchronized (consumer) {
                if (messageListenerService != null) {
                    messageListenerService.close();
                }
                consumer.unsubscribe();
                consumer.close();
            }
            LOG.info("KafkaConsumer closed.");
        }
    }

    /**
     * 启动信息，方便日后排查问题
     * @return
     */
    private String startupInfo(){
        boolean isAssignTopicPartition = !CollectionUtils.isEmpty(configs.getTopicPartitions());
        StringBuilder builder = new StringBuilder(200);
        builder.append("bootstrap.servers : ").append(StringUtils.isBlank(configs.getKafkaServers()) ? configs.getZookeeperServers() : configs.getKafkaServers()).append(" , ");
        builder.append("group.id : ").append(configs.getGroupId()).append(" , ");
        builder.append("in ").append(isAssignTopicPartition ? "[assign] : " + configs.getTopicPartitions(): "[subscribe] : " + configs.getTopic()).append(" , ");
        builder.append("with listener service : " + messageListenerService.getClass().getSimpleName()).append(" ");
        return builder.toString();
    }
}
