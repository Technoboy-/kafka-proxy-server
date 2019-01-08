package com.tt.kafka.push.server.biz;

import com.tt.kafka.client.transport.Connection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class ClientRegistry {

    public static final ClientRegistry I = new ClientRegistry();

    private final ConcurrentHashMap<String, Connection> registry = new ConcurrentHashMap<>();

    public void register(Connection connection){
        registry.putIfAbsent(connection.getId().asLongText(), connection);
    }

    public void unregister(Connection connection){
        registry.remove(connection.getId().asLongText());
    }

    public List<Connection> getCopyClients(){
        return new ArrayList<>(registry.values());
    }

}
