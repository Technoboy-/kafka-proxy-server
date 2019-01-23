package com.owl.kafka.push.server.biz.service;

import com.owl.kafka.proxy.transport.exceptions.ChannelInactiveException;

/**
 * @Author: Tboy
 */
public interface RepushPolicy<T> {

    void start();

    void repush(T msg) throws InterruptedException, ChannelInactiveException;
}
