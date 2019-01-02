package com.tt.kafka.push.server.consumer;

import com.tt.kafka.consumer.ConsumerConfig;
import com.tt.kafka.consumer.exceptions.TopicNotExistException;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.metric.MonitorImpl;
import com.tt.kafka.util.CollectionUtils;
import com.tt.kafka.util.Preconditions;
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
public class PushServerKafkaConsumer<K, V> implements Runnable, com.tt.kafka.consumer.KafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(PushServerKafkaConsumer.class);

    private final AtomicBoolean start = new AtomicBoolean(false);

    private Consumer<byte[], byte[]> consumer;

    private final Thread worker = new Thread(this, "pusher-server-consumer-poll-worker");

    private final ConsumerConfig configs;

    private final MessageListenerService messageListenerService;

    public PushServerKafkaConsumer(ConsumerConfig configs) {
        this.configs = configs;

        // KAFKA 0.11 later version.
        if(configs.get("partition.assignment.strategy") == null){
            configs.put("partition.assignment.strategy", "com.tt.kafka.consumer.assignor.CheckTopicStickyAssignor");
        }
        configs.put("bootstrap.servers", configs.getBootstrapServers());
        configs.put("group.id", configs.getGroupId());

        this.consumer = new KafkaConsumer(configs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        this.messageListenerService = new PushServerMessageListenerService();
        
    }

    @Override
    public void setMessageListener(MessageListener messageListener) {
        throw new UnsupportedOperationException("not support MessageListener in push server");
    }

    public void start() {

        Preconditions.checkArgument(CollectionUtils.isEmpty(configs.getTopicPartitions()), "topicPartition is not support in push server");

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
        builder.append("bootstrap.servers : ").append(configs.getBootstrapServers()).append(" , ");
        builder.append("group.id : ").append(configs.getGroupId()).append(" , ");
        builder.append("in ").append(isAssignTopicPartition ? "[assign] : " + configs.getTopicPartitions(): "[subscribe] : " + configs.getTopic()).append(" , ");
        builder.append("with listener service : " + messageListenerService.getClass().getSimpleName()).append(" ");
        return builder.toString();
    }
}
