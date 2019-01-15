package com.owl.kafka.push.server.biz.registry;

import com.owl.kafka.client.service.RegistryService;
import com.owl.kafka.push.server.biz.service.InstanceHolder;

/**
 * @Author: Tboy
 */
public class RegistryCenter {

    public static RegistryCenter I = new RegistryCenter();

    private final ServerRegistry serverRegistry;

    private final ClientRegistry clientRegistry;

    private RegistryCenter(){
        RegistryService registryService = new RegistryService();
        this.serverRegistry = new ServerRegistry(registryService);
        this.clientRegistry = new ClientRegistry(registryService);
        InstanceHolder.I.setRegistryService(registryService);
    }

    public ServerRegistry getServerRegistry() {
        return serverRegistry;
    }

    public ClientRegistry getClientRegistry() {
        return clientRegistry;
    }
}
