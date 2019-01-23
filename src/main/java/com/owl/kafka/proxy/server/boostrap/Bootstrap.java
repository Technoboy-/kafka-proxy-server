package com.owl.kafka.proxy.server.boostrap;

import com.owl.kafka.proxy.server.biz.push.PushServer;
import com.owl.kafka.proxy.server.biz.pull.PullServer;

/**
 * @Author: Tboy
 */
public class Bootstrap {

    public static void main(String[] args) {
        startPullServer();
//        startPushServer();
    }

    private static void startPullServer(){
        //
        PullServer pullServer = new PullServer();
        //
        pullServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pullServer.close();
        }));
    }

    private static void startPushServer(){
        //
        PushServer pushServer = new PushServer();
        //
        pushServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pushServer.close();
        }));
    }


}
