package com.tt.kafka.push.server.netty;

/**
 * @Author: Tboy
 */
public interface LoadBalancePolicy<T> {

    T get();
}
