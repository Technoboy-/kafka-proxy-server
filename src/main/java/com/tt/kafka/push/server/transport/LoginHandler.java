package com.tt.kafka.push.server.transport;

import com.tt.kafka.client.transport.handler.CommonMessageHandler;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.push.server.PushServerExecutor;
import com.tt.kafka.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class LoginHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoginHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        PushServerExecutor.EXECUTOR.execute(new LoginTask(connection, packet));
    }

    class LoginTask implements Runnable{

        private Packet packet;
        private Connection connection;
        public LoginTask(Connection connection, Packet packet){
            this.connection = connection;
            this.packet = packet;
        }

        @Override
        public void run() {
            try {
                Packet response = new Packet();
                response.setVersion(packet.getVersion());
                response.setCmd(packet.getCmd());
                response.setMsgId(packet.getMsgId());
                response.setHeader(new byte[0]);
                response.setKey(new byte[0]);
                response.setValue(new byte[0]);
                connection.send(response);
                ClientRegistry.I.register(connection);
            } catch (Exception ex){
                LOGGER.error("LoginTask error {}, close channel [ip:{}], ", ex, NetUtils.getRemoteAddress(connection.getChannel()));
                connection.close();
            }
        }
    }
}
