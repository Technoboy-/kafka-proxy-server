package com.tt.kafka.push.server.biz.service;

import com.tt.kafka.client.transport.exceptions.ChannelInactiveException;

/**
 * @Author: Tboy
 */
public interface RepushPolicy<T> {

    void repush(T msg) throws InterruptedException, ChannelInactiveException;
}
