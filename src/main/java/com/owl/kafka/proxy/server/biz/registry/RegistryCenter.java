package com.owl.kafka.proxy.server.biz.registry;

import com.owl.kafka.client.proxy.service.RegistryService;
import com.owl.kafka.client.proxy.zookeeper.ZookeeperClient;
import com.owl.kafka.proxy.server.biz.bo.ServerConfigs;
import com.owl.kafka.proxy.server.biz.service.InstanceHolder;

/**
 * @Author: Tboy
 */
public class RegistryCenter {

    private final ServerRegistry serverRegistry;

    private final ClientRegistry clientRegistry;

    private final ZookeeperClient zookeeperClient;

    private final RegistryService registryService;

    public RegistryCenter(){
        this.zookeeperClient = new ZookeeperClient(ServerConfigs.I.getZookeeperServerList(), ZookeeperClient.PUSH_SERVER_NAMESPACE, ServerConfigs.I.getZookeeperSessionTimeoutMs(), ServerConfigs.I.getZookeeperConnectionTimeoutMs());
        this.registryService = new RegistryService(this.zookeeperClient);
        //
        this.serverRegistry = new ServerRegistry(registryService);
        this.clientRegistry = new ClientRegistry(registryService);

        //
        InstanceHolder.I.setZookeeperClient(this.zookeeperClient);
        InstanceHolder.I.setRegistryCenter(this);
    }

    public ServerRegistry getServerRegistry() {
        return serverRegistry;
    }

    public ClientRegistry getClientRegistry() {
        return clientRegistry;
    }

    public void close(){
        this.serverRegistry.unregister();
        this.registryService.close();
        this.zookeeperClient.close();
    }
}
