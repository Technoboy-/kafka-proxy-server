package com.tt.kafka.push.server.boostrap;

import com.tt.kafka.client.PushConfigs;

/**
 * @Author: Tboy
 */
public class Bootstrap {

    public static void main(String[] args) {
        //
        PushConfigs serverConfigs = new PushConfigs(true);

        //
        PushTcpServer pushTcpServer = new PushTcpServer(serverConfigs);
        pushTcpServer.start();
        //
        PushServer pushServer = new PushServer(serverConfigs);
        pushServer.setPushTcpServer(pushTcpServer);
        pushServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pushServer.close();
            pushTcpServer.close();
        }));
    }

}
