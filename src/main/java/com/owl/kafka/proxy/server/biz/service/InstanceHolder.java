package com.owl.kafka.proxy.server.biz.service;

import com.owl.kafka.client.proxy.zookeeper.ZookeeperClient;
import com.owl.kafka.proxy.server.biz.registry.RegistryCenter;

/**
 * @Author: Tboy
 */
public class InstanceHolder {

    public static InstanceHolder I = new InstanceHolder();

    private DLQService dlqService;

    private ZookeeperClient zookeeperClient;

    private RegistryCenter registryCenter;

    public RegistryCenter getRegistryCenter() {
        return registryCenter;
    }

    public void setRegistryCenter(RegistryCenter registryCenter) {
        this.registryCenter = registryCenter;
    }

    public void setDLQService(DLQService service){
        this.dlqService = service;
    }

    public DLQService getDLQService(){
        return this.dlqService;
    }

    public ZookeeperClient getZookeeperClient() {
        return zookeeperClient;
    }

    public void setZookeeperClient(ZookeeperClient zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
    }
}
