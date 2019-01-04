package com.tt.kafka.push.server.transport.handler;

import com.tt.kafka.client.transport.handler.CommonMessageHandler;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.push.server.transport.ClientRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class RegisterMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        ClientRegistry.I.register(connection);
    }
}
