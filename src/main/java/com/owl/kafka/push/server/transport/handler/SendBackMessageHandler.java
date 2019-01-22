package com.owl.kafka.push.server.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.push.server.biz.pull.PullCenter;
import com.owl.kafka.push.server.biz.registry.RegistryCenter;
import com.owl.kafka.push.server.biz.service.InstanceHolder;
import com.owl.kafka.serializer.SerializerImpl;
import com.owl.kafka.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class SendBackMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendBackMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received sendback message : {}, from : {}", packet, NetUtils.getRemoteAddress(connection.getChannel()));
        }
        //TODO
//        Header header = (Header)SerializerImpl.getFastJsonSerializer().deserialize(packet.getHeader(), Header.class);
//        if(header.getRepost() >= 10){
//            InstanceHolder.I.getDLQService().write(header.getMsgId(), packet);
//        } else{
//            header.setRepost((byte)(header.getRepost() + 1));
//            packet.setHeaderRef(header);
//            PullCenter.I.reputMessage(packet);
//        }

    }
}
