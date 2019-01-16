package com.owl.kafka.push.server.biz.registry;

import com.owl.kafka.client.service.RegistryService;
import com.owl.kafka.client.zookeeper.ZookeeperClient;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.push.server.biz.service.InstanceHolder;

/**
 * @Author: Tboy
 */
public class RegistryCenter {

    public static RegistryCenter I = new RegistryCenter();

    private final ServerRegistry serverRegistry;

    private final ClientRegistry clientRegistry;

    private final ZookeeperClient zookeeperClient;

    private final RegistryService registryService;

    private RegistryCenter(){
        this.zookeeperClient = new ZookeeperClient(ServerConfigs.I.getZookeeperServerList(), ZookeeperClient.PUSH_SERVER_NAMESPACE, ServerConfigs.I.getZookeeperSessionTimeoutMs(), ServerConfigs.I.getZookeeperConnectionTimeoutMs());
        this.registryService = new RegistryService(this.zookeeperClient);
        //
        this.serverRegistry = new ServerRegistry(registryService);
        this.clientRegistry = new ClientRegistry(registryService);

        //
        InstanceHolder.I.setZookeeperClient(this.zookeeperClient);
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
