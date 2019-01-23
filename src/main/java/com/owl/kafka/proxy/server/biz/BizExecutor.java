package com.owl.kafka.proxy.server.biz;

import com.owl.kafka.client.util.CallerWaitPolicy;
import com.owl.kafka.client.util.Constants;
import com.owl.kafka.client.util.NamedThreadFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class BizExecutor {

    private static final int CORE_SIZE = Constants.CPU_SIZE << 1;

    public static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(
            CORE_SIZE, 100, 5, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(10000),
            new NamedThreadFactory("push-server-executor"),
            new CallerWaitPolicy());

    public void close(){
        EXECUTOR.shutdown();
    }
}
