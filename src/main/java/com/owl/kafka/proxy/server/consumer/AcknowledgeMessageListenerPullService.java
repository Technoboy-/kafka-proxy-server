package com.owl.kafka.proxy.server.consumer;

import com.owl.kafka.client.consumer.service.RebalanceMessageListenerService;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerPullService<K, V> extends RebalanceMessageListenerService<K, V> {

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        try {
            PullCenter.I.putMessage(record);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        PullCenter.I.close();
    }

}
