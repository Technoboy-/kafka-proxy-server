package com.tt.kafka.push.server.biz;

import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.client.service.LoadBalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
public class RoundRobinLoadBalance implements LoadBalance<Connection> {

    private final AtomicInteger index = new AtomicInteger(0);

    public Connection select(List<Connection> invokers) {
        if(invokers.size() <= 0){
            return null;
        }
        return invokers.get(index.incrementAndGet() % invokers.size());
    }
}
