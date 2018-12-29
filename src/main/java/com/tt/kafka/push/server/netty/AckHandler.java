package com.tt.kafka.push.server.netty;

import com.tt.kafka.netty.handler.CommonMessageHandler;
import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.transport.Connection;
import com.tt.kafka.push.server.PushServerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class AckHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        PushServerExecutor.EXECUTOR.execute(new AckTask(connection, packet));

    }

    class AckTask implements Runnable{

        private Connection connection;

        private Packet packet;

        public AckTask(Connection connection, Packet packet){
            this.connection = connection;
            this.packet = packet;
        }

        @Override
        public void run() {
            //TODO
        }
    }
}
