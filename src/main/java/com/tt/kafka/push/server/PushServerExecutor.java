package com.tt.kafka.push.server;

import com.tt.kafka.util.CallerWaitPolicy;
import com.tt.kafka.util.NamedThreadFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import static com.tt.kafka.util.Constants.CPU_SIZE;

/**
 * @Author: Tboy
 */
public class  PushServerExecutor {

    private static final int CORE_SIZE = CPU_SIZE << 1;

    public static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(
            CORE_SIZE, 100, 5, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(10000),
            new NamedThreadFactory("push-server-executor"),
            new CallerWaitPolicy());

    public void close(){
        EXECUTOR.shutdown();
    }
}
