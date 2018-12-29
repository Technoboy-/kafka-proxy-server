package com.tt.kafka.push.server.netty;

import com.tt.kafka.netty.handler.CommonMessageHandler;
import com.tt.kafka.netty.protocol.Command;
import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.transport.Connection;
import com.tt.kafka.push.server.PushServerExecutor;
import com.tt.kafka.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class HeartbeatHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        PushServerExecutor.EXECUTOR.execute(new HeartbeatTask(connection, packet));
    }

    class HeartbeatTask implements Runnable{

        private Packet packet;
        private Connection connection;
        public HeartbeatTask(Connection connection, Packet packet){
            this.packet = packet;
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                Packet ack = new Packet();
                ack.setVersion(packet.getVersion());
                ack.setMsgId(packet.getMsgId());
                ack.setCmd(Command.HEARTBEAT_ACK.getCmd());
                ack.setHeader(new byte[0]);
                ack.setKey(new byte[0]);
                ack.setValue(new byte[0]);
                connection.send(ack);
            } catch (Exception ex){
                LOGGER.error("HeartbeatTask error {}, close channel [ip:{}], ", ex, NetUtils.getRemoteAddress(connection.getChannel()));
                connection.close();
            }
        }
    }
}
