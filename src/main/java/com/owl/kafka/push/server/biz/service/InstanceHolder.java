package com.owl.kafka.push.server.biz.service;

import com.owl.kafka.client.zookeeper.ZookeeperClient;

/**
 * @Author: Tboy
 */
public class InstanceHolder {

    public static InstanceHolder I = new InstanceHolder();

    private DLQService dlqService;

    private ZookeeperClient zookeeperClient;

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
