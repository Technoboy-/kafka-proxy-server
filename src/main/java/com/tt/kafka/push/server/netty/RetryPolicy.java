package com.tt.kafka.push.server.netty;

/**
 * @Author: Tboy
 */
public interface RetryPolicy {

    boolean allowRetry() throws InterruptedException;

    void reset();

}
