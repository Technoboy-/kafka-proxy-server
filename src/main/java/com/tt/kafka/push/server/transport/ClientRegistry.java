package com.tt.kafka.push.server.transport;

import com.tt.kafka.client.transport.Connection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @Author: Tboy
 */
public class ClientRegistry {

    public static final ClientRegistry I = new ClientRegistry();

    private final List<Connection> registry = new ArrayList<>();

    public void register(Connection connection){
        registry.add(connection);
    }

    public void unregister(Connection connection){
        registry.remove(connection);
    }

    public List<Connection> getCopyClients(){
        return new ArrayList<>(registry);
    }

}
