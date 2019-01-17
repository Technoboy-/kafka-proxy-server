package com.owl.kafka.push.server.consumer;

import com.owl.kafka.push.server.biz.PushCenter;
import com.owl.kafka.consumer.service.RebalanceMessageListenerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerPushService<K, V> extends RebalanceMessageListenerService<K, V>{

    private final PushCenter pushCenter;

    public AcknowledgeMessageListenerPushService(PushCenter pushCenter){
        this.pushCenter = pushCenter;
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
        //NOP
    }

}
