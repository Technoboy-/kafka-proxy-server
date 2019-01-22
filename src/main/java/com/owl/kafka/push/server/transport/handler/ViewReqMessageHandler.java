package com.owl.kafka.push.server.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.push.server.biz.service.InstanceHolder;
import com.owl.kafka.serializer.SerializerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class ViewReqMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViewReqMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received view message : {}", packet);
        //todo
//        Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(packet.getHeader(), Header.class);
//        Record<byte[], byte[]> record = InstanceHolder.I.getDLQService().view(header.getMsgId());
//        if(record != null){
//            connection.send(Packets.toViewPacket(header.getMsgId(), record));
//        }
    }


}
