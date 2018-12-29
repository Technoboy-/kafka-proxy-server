package com.tt.kafka.push.server.netty;

import com.tt.kafka.netty.transport.Connection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
public class LoadBalanceInvokePolicy implements InvokePolicy {

    private final AtomicInteger index = new AtomicInteger(0);

    private final PushClientRegistry clientRegistry;

    public LoadBalanceInvokePolicy(PushClientRegistry clientRegistry){
        this.clientRegistry = clientRegistry;
    }

    @Override
    public Connection getConnection() {
        Collection<Connection> values = clientRegistry.getClients();
        List<Connection> clients = new ArrayList<>(values);
        if(clients.size() <= 0){
            return null;
        }
        return clients.get(index.incrementAndGet() % clients.size());
    }
}
