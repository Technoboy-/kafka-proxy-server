package com.owl.kafka.proxy.server.transport.handler;

import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.proxy.transport.message.Header;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.MessageCodec;
import com.owl.kafka.client.util.NetUtils;
import com.owl.kafka.proxy.server.biz.bo.ServerConfigs;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import com.owl.kafka.proxy.server.biz.service.InstanceHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @Author: Tboy
 */
public class SendBackMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendBackMessageHandler.class);

    private final int repostCount = ServerConfigs.I.getServerRepostCount();

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        Message message = MessageCodec.decode(packet.getBody());
        Header header = message.getHeader();
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received sendback message : {}, from : {}", header, NetUtils.getRemoteAddress(connection.getChannel()));
        }
        if(header.getRepost() >= repostCount){
            InstanceHolder.I.getDLQService().write(header.getMsgId(), packet);
        } else{
            header.setRepost((byte)(header.getRepost() + 1));
            ByteBuffer buffer = MessageCodec.encode(message);
            packet.setBody(buffer);
            PullCenter.I.reputMessage(packet);
        }

    }
}
