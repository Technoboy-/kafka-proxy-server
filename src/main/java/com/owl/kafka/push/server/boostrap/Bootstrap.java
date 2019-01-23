package com.owl.kafka.push.server.boostrap;

import com.owl.kafka.push.server.biz.pull.PullServer;
import com.owl.kafka.push.server.biz.push.PushServer;

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
