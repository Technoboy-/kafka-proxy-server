package com.owl.kafka.push.server.biz.service;

import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.zookeeper.ZookeeperClient;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.push.server.biz.bo.ResendPacket;
import com.owl.kafka.push.server.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.util.Preconditions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class DLQService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultKafkaConsumerImpl.class);

    private static final String DLQ_DATA_PATH = "/%s";  //-dlq/msgId

    private final Consumer<byte[], byte[]> consumer;

    private final ZookeeperClient zookeeperClient;

    private final Producer<byte[], byte[]> producer;

    private final String topic;

    public DLQService(String bootstrapServers, String topic, String groupId, String zookeeperServers){
        //config for consumer
        this.topic = topic + "-dlq";
        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put("bootstrap.servers", bootstrapServers);
        consumerConfigs.put("group.id", groupId);
        consumerConfigs.put("fetch.max.bytes", 10 * 1024 * 1024); //10m for a request, only fetch one record
        consumerConfigs.put("enable.auto.commit", true);
        this.consumer = new KafkaConsumer(consumerConfigs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        this.consumer.subscribe(Arrays.asList(this.topic));
        //config for producer
        Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put("bootstrap.servers", bootstrapServers);
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer(producerConfigs);
        //zk
        this.zookeeperClient = new ZookeeperClient(zookeeperServers, ZookeeperClient.PUSH_SERVER_NAMESPACE, 30000, 15000);
    }

    public void close(){
        this.producer.close();
        this.consumer.close();
        this.zookeeperClient.close();
    }

    public Record<byte[], byte[]> view(long msgId){
        try {
            String dlp = String.format(this.topic + DLQ_DATA_PATH, msgId);
            byte[] data = zookeeperClient.getData(dlp);
            ByteBuffer wrap = ByteBuffer.wrap(data);
            long offset = wrap.getLong();
            TopicPartition topicPartition = new TopicPartition(this.topic, 0);
            consumer.seek(topicPartition, offset - 1);
            ConsumerRecords<byte[], byte[]> records;
            while((records = consumer.poll(0)) != null){
                Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
                while(iterator.hasNext()){
                    ConsumerRecord<byte[], byte[]> next = iterator.next();
                    if(next.offset() == offset){
                        Record<byte[], byte[]> record = new Record<>(msgId, this.topic, 0, offset, next.key(), next.value(), next.timestamp());
                        return record;
                    }
                }
            }
        } catch (Exception ex){
            LOG.error("view error", ex);
        }
        return null;
    }

    public void write(ResendPacket resendPacket){
        Preconditions.checkArgument(resendPacket.getRepost() >= 10, "resendPacket must repost more than 3 times");
        try {
            Packet packet = resendPacket.getPacket();
            String dlp = String.format(this.topic + DLQ_DATA_PATH, resendPacket.getMsgId());
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(this.topic, 0, packet.getKey(), packet.getValue());
            this.producer.send(record, new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        try {
                            zookeeperClient.createPersistent(dlp, toByteArray(metadata.offset()));
                        } catch (Exception ex) {
                            LOG.error("write to zk path : {}, data : {}, error : {}", new Object[]{dlp, metadata.offset(), ex});
                        }
                    } else {
                        LOG.error("write to kafka error", exception);
                    }
                }
            });
        } catch (Exception ex){
            LOG.error("write error", ex);
        }
    }

    private byte[] toByteArray(long offset){
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(offset);
        return buffer.array();
    }


}


