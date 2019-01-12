package com.owl.kafka.push.server.boostrap;

import com.owl.kafka.push.server.biz.PushServer;

/**
 * @Author: Tboy
 */
public class Bootstrap {

    public static void main(String[] args) {
        //
        PushServer pushServer = new PushServer();
        //
        pushServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pushServer.close();
        }));
    }

}
