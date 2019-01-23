package com.owl.kafka.proxy.server.biz.service;


import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;

/**
 * @Author: Tboy
 */
public interface RepushPolicy<T> {

    void start();

    void repush(T msg) throws InterruptedException, ChannelInactiveException;
}
