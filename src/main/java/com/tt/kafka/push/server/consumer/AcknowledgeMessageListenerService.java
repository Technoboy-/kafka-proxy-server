package com.tt.kafka.push.server.consumer;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.consumer.service.RebalanceMessageListenerService;
import com.tt.kafka.push.server.biz.PushCenter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

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
