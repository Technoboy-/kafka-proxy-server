package com.owl.kafka.push.server.biz.registry;

import com.owl.kafka.client.service.RegisterMetadata;
import com.owl.kafka.client.service.RegistryService;
import com.owl.kafka.client.transport.Address;
import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @Author: Tboy
 */
public class ClientRegistry {

    private final CopyOnWriteArraySet<Connection> localRegistry = new CopyOnWriteArraySet<>();

    private final RegistryService registryService;

    public ClientRegistry(RegistryService registryService){
        this.registryService = registryService;
    }

    public void register(Connection connection){
        localRegistry.add(connection);
        //
        RegisterMetadata metadata = toRegisterMetadata(connection);
        registryService.register(metadata);

    }

    public void unregister(Connection connection){
        localRegistry.remove(connection.getId().asLongText());
        //
        RegisterMetadata metadata = toRegisterMetadata(connection);
        registryService.unregister(metadata);
    }

    private RegisterMetadata toRegisterMetadata(Connection connection){
        RegisterMetadata metadata = new RegisterMetadata();
        metadata.setPath(String.format(ServerConfigs.I.ZOOKEEPER_CONSUMERS, ServerConfigs.I.getServerTopic()));
        InetSocketAddress remoteAddress = ((InetSocketAddress)connection.remoteAddress());
        Address address = new Address(remoteAddress.getHostName(), remoteAddress.getPort());
        metadata.setAddress(address);
        return metadata;
    }

    public List<Connection> getClients(){
        return new ArrayList<>(localRegistry);
    }

}
