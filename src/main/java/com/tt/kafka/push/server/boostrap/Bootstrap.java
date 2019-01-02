package com.tt.kafka.push.server.boostrap;

import com.tt.kafka.consumer.ConsumerConfig;
import com.tt.kafka.consumer.KafkaConsumer;
import com.tt.kafka.push.server.consumer.PushServerKafkaConsumer;

/**
 * @Author: Tboy
 */
public class Bootstrap {

    public static void main(String[] args) {
        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic", "test-group");
        configs.setAutoCommit(true);
        KafkaConsumer consumer = new PushServerKafkaConsumer(configs);
        consumer.start();
    }
}
