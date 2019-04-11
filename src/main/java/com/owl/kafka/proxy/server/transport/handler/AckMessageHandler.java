package com.owl.kafka.proxy.server.transport.handler;


import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.proxy.transport.message.Header;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.MessageCodec;
import com.owl.kafka.client.util.NamedThreadFactory;
import com.owl.kafka.proxy.server.biz.bo.ServerConfigs;
import com.owl.kafka.proxy.server.consumer.ProxyConsumer;
import com.owl.kafka.proxy.server.biz.service.MessageHolder;
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

    private final ProxyConsumer consumer;

    private final ScheduledExecutorService commitScheduler;

    private final int interval = ServerConfigs.I.getServerCommitOffsetInterval();

    private final int batchSize = ServerConfigs.I.getServerCommitOffsetBatchSize();

    public AckMessageHandler(ProxyConsumer consumer){
        this.consumer = consumer;
        this.commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));
        this.commitScheduler.scheduleAtFixedRate(new CommitOffsetTask(), interval, interval, TimeUnit.SECONDS);
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        Message message = MessageCodec.decode(packet.getBody());
        Header.Sign sign = Header.Sign.of(message.getHeader().getSign());
        if(sign == null){
            LOGGER.error("sign is empty, opaque : {}, message : {}", packet.getOpaque(),message);
            return;
        }
        switch (sign){
            case PUSH:
                LOGGER.debug("received push ack msg : {}", message);
                acknowledge(message.getHeader());
                boolean result = MessageHolder.fastRemove(message);
                if(!result){
                    LOGGER.warn("MessageHolder not found ack opaque : {}, just ignore", packet.getOpaque());
                }
                break;
            case PULL:
                LOGGER.debug("received pull ack msg : {}", message);
                acknowledge(message.getHeader());
                break;

        }
    }

    protected void acknowledge(Header header){
        if (messageCount.incrementAndGet() % batchSize == 0) {
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
                consumer.getMetricsMonitor().recordCommitCount(1L);
                consumer.getMetricsMonitor().recordCommitTime(System.currentTimeMillis() - now);
            }
        }
    }
}
