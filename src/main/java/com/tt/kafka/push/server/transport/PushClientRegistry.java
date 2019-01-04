package com.tt.kafka.push.server.transport;

import com.tt.kafka.client.transport.Connection;
import io.netty.channel.ChannelId;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class PushClientRegistry {

    private ConcurrentHashMap<ChannelId, Connection> registry = new ConcurrentHashMap<>();

    public void register(Connection connection){
        registry.putIfAbsent(connection.getId(), connection);
    }

    public Collection<Connection> getClients(){
        return registry.values();
    }

}
