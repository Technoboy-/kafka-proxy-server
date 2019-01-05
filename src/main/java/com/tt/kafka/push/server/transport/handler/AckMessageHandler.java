package com.tt.kafka.push.server.transport.handler;

import com.tt.kafka.client.transport.handler.CommonMessageHandler;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.push.server.transport.MemoryQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class AckMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received ack msgId : {}", packet.getMsgId());
        MemoryQueue.ackQueue.remove(packet);
        LOGGER.debug("ackQueue size : {}", MemoryQueue.ackQueue.size());
    }
}
