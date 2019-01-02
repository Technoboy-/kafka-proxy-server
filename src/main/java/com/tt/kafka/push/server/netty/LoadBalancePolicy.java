package com.tt.kafka.push.server.netty;

import com.tt.kafka.netty.transport.Connection;

/**
 * @Author: Tboy
 */
public interface LoadBalancePolicy {

    Connection getConnection();
}
