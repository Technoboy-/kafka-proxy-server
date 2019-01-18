package com.owl.kafka.push.server.consumer;

import com.owl.kafka.consumer.service.RebalanceMessageListenerService;
import com.owl.kafka.push.server.biz.pull.PullCenter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerPullService<K, V> extends RebalanceMessageListenerService<K, V>{

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        try {
            PullCenter.I.getPullQueue().put(record);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        //NOP
    }

}
