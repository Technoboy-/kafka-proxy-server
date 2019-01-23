package com.owl.kafka.push.server.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.transport.message.Message;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.MessageCodec;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.push.server.biz.service.InstanceHolder;
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
        Message message = MessageCodec.decode(packet.getBody());
        Header header = message.getHeader();
        Record<byte[], byte[]> record = InstanceHolder.I.getDLQService().view(header.getMsgId());
        if(record != null){
            connection.send(Packets.viewResp(packet.getOpaque(), header.getMsgId(), record));
        } else{
            connection.send(Packets.noViewMsgResp(packet.getOpaque()));
        }
    }


}
