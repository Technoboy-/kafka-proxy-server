package com.owl.kafka.push.server.biz.service;

import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.zookeeper.ZookeeperClient;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.push.server.biz.bo.ResendPacket;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.push.server.consumer.DLQConsumer;
import com.owl.kafka.push.server.consumer.PushServerConsumer;
import com.owl.kafka.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class DLQService {

    private static final Logger LOG = LoggerFactory.getLogger(PushServerConsumer.class);

    private static final String DLQ_DATA_PATH = "/%s";  //-dlq/msgId

    private DLQConsumer dlqConsumer;

    private final ZookeeperClient zookeeperClient;

    private final Producer<byte[], byte[]> producer;

    private final String topic;

    public DLQService(String bootstrapServers, String topic, String groupId, String zookeeperServers){
        //config for consumer
        this.topic = topic + "-dlq";

        //config for producer
        Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put("bootstrap.servers", bootstrapServers);
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer(producerConfigs);

        this.zookeeperClient = new ZookeeperClient(zookeeperServers, ZookeeperClient.PUSH_SERVER_NAMESPACE, 30000, 15000);

        this.dlqConsumer = new DLQConsumer(bootstrapServers, this.topic, groupId, zookeeperClient);
    }

    public void close(){
        this.producer.close();
        this.dlqConsumer.close();
        this.zookeeperClient.close();
    }

    public Record<byte[], byte[]> view(long msgId){
        try {
            String dlp = String.format(this.topic + DLQ_DATA_PATH, msgId);
            byte[] data = zookeeperClient.getData(dlp);
            ByteBuffer wrap = ByteBuffer.wrap(data);
            long offset = wrap.getLong();
            ConsumerRecord<byte[], byte[]> record = dlqConsumer.seek(offset);
            if(record != null){
                Record<byte[], byte[]> r = new Record<>(msgId, record.topic(), 0, offset, record.key(), record.value(), record.timestamp());
                return r;
            }
        } catch (Exception ex){
            LOG.error("view error", ex);
        }
        return null;
    }

    public void write(ResendPacket resendPacket){
        Preconditions.checkArgument(resendPacket.getRepost() >= ServerConfigs.I.getServerMessageRepostTimes(), "resendPacket must repost more than " + ServerConfigs.I.getServerMessageRepostTimes() + " times");
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


