package com.tt.kafka.push.server.biz.service;

/**
 * @Author: Tboy
 */
public interface RepushPolicy<T> {

    void repush(T msg) throws InterruptedException;
}
