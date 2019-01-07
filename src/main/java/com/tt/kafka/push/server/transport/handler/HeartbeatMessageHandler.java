package com.tt.kafka.push.server.transport.handler;

import com.tt.kafka.client.transport.handler.CommonMessageHandler;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.push.server.transport.ClientRegistry;
import com.tt.kafka.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class HeartbeatMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isTraceEnabled()){
            LOGGER.trace("received heartbeat :{}, from : {}", packet, NetUtils.getRemoteAddress(connection.getChannel()));
        }
        ClientRegistry.I.register(connection);
    }

}
