package com.owl.kafka.push.server.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.metric.MonitorImpl;
import com.owl.kafka.push.server.biz.service.InstanceHolder;
import com.owl.kafka.push.server.biz.service.MessageHolder;
import com.owl.kafka.push.server.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.serializer.SerializerImpl;
import com.owl.kafka.util.NamedThreadFactory;
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
public class ViewMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViewMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received view msgId : {}", packet.getMsgId());
        Record<byte[], byte[]> record = InstanceHolder.I.getDLQService().view(packet.getMsgId());
        if(record != null){
            connection.send(Packets.toViewPacket(packet.getMsgId(), record));
        }
    }


}
