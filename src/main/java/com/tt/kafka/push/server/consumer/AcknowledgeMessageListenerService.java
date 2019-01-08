package com.tt.kafka.push.server.consumer;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.consumer.service.RebalanceMessageListenerService;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Header;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.service.IdService;
import com.tt.kafka.push.server.biz.PushCenter;
import com.tt.kafka.push.server.transport.MemoryQueue;
import com.tt.kafka.serializer.SerializerImpl;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerService<K, V> extends RebalanceMessageListenerService<K, V>{

    private final PushCenter pushCenter;

    public AcknowledgeMessageListenerService(PushConfigs serverConfigs){
        this.pushCenter = new PushCenter(serverConfigs);
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        try {
            pushCenter.getPushQueue().put(record);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        this.pushCenter.close();
    }

}
