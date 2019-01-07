package com.tt.kafka.push.server.transport.handler;

import com.tt.kafka.client.transport.handler.CommonMessageHandler;
import com.tt.kafka.client.transport.protocol.Header;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.metric.MonitorImpl;
import com.tt.kafka.push.server.consumer.PushServerConsumer;
import com.tt.kafka.push.server.transport.MemoryQueue;
import com.tt.kafka.serializer.SerializerImpl;
import com.tt.kafka.util.NamedThreadFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class AckMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckMessageHandler.class);

    private final boolean isAcknowledge;

    protected ScheduledExecutorService commitScheduler;

    protected volatile ConcurrentMap<TopicPartition, OffsetAndMetadata> latestOffsetMap = new ConcurrentHashMap<>();

    protected final AtomicLong messageCount = new AtomicLong();

    private final PushServerConsumer consumer;

    public AckMessageHandler(PushServerConsumer consumer, boolean isAcknowledge){
        this.consumer = consumer;
        this.isAcknowledge = isAcknowledge;
        if(isAcknowledge){
            commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));
        }
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received ack msgId : {}", packet.getMsgId());
        Packet remove = MemoryQueue.ackMap.remove(packet.getMsgId());
        if(remove != null && isAcknowledge){
            Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(remove.getHeader(), Header.class);
            acknowledge(header);
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
