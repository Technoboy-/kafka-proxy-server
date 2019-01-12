package com.owl.kafka.push.server.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.metric.MonitorImpl;
import com.owl.kafka.push.server.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.push.server.biz.service.MessageHolder;
import com.owl.kafka.serializer.SerializerImpl;
import com.owl.kafka.util.NamedThreadFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class AckMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckMessageHandler.class);

    private volatile ConcurrentMap<TopicPartition, OffsetAndMetadata> latestOffsetMap = new ConcurrentHashMap<>();

    private final AtomicLong messageCount = new AtomicLong(1);

    private final DefaultKafkaConsumerImpl consumer;

    private final ScheduledExecutorService commitScheduler;

    public AckMessageHandler(DefaultKafkaConsumerImpl consumer){
        this.consumer = consumer;
        this.commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));
        this.commitScheduler.scheduleAtFixedRate(new CommitOffsetTask(), 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received ack msgId : {}", packet.getMsgId());
        Packet remove = MessageHolder.fastRemove(packet);
        if(remove.getHeader() != null && remove.getHeader().length > 0){
            Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(remove.getHeader(), Header.class);
            acknowledge(header);
        } else{
            LOGGER.warn("MessageHolder not found ack msgId : {}, just ignore", packet.getMsgId());
        }
    }

    protected void acknowledge(Header header){
        if (messageCount.incrementAndGet() % 10000 == 0) {
            commitScheduler.execute(new CommitOffsetTask());
        }
        toOffsetMap(header);
    }

    private void toOffsetMap(Header header){
        TopicPartition topicPartition = new TopicPartition(header.getTopic(), header.getPartition());
        OffsetAndMetadata offsetAndMetadata = latestOffsetMap.get(topicPartition);
        if (offsetAndMetadata == null || header.getOffset() > offsetAndMetadata.offset()) {
            latestOffsetMap.put(topicPartition, new OffsetAndMetadata(header.getOffset()));
        }
    }

    class CommitOffsetTask implements Runnable {

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            try {
                final Map<TopicPartition, OffsetAndMetadata> pre = latestOffsetMap;
                latestOffsetMap = new ConcurrentHashMap<>();
                if (pre.isEmpty()) {
                    return;
                }
                consumer.commit(pre);
            } catch (Throwable ex) {
                LOGGER.error("Commit consumer offset error.", ex);
            } finally {
                MonitorImpl.getDefault().recordCommitCount(1L);
                MonitorImpl.getDefault().recordCommitTime(System.currentTimeMillis() - now);
            }
        }
    }
}
