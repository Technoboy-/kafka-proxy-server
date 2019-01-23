package com.owl.kafka.proxy.server.transport.handler;


import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.client.util.NetUtils;
import com.owl.kafka.proxy.server.biz.service.InstanceHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PingMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PingMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received ping : {}, from : {}", packet, NetUtils.getRemoteAddress(connection.getChannel()));
        }
        connection.send(Packets.pong());
        InstanceHolder.I.getRegistryCenter().getClientRegistry().register(connection);
    }

}
