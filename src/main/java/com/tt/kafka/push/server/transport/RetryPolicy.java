package com.tt.kafka.push.server.transport;

/**
 * @Author: Tboy
 */
public interface RetryPolicy {

    boolean allowRetry() throws InterruptedException;

    void reset();

}
