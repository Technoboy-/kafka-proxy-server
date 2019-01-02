package com.tt.kafka.push.server.zookeeper;

import com.tt.kafka.push.server.PushServerConfigs;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * @Author: Tboy
 */
public class ZookeeperClient {

    private final CuratorFramework client;

    public ZookeeperClient(PushServerConfigs serverConfigs){
        this.client = CuratorFrameworkFactory.builder()
                .namespace(serverConfigs.getZookeeperNamespace())
                .connectString(serverConfigs.getZookeeperServerList())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(serverConfigs.getZookeeperSessionTimeoutMs())
                .connectionTimeoutMs(serverConfigs.getZookeeperConnectionTimeoutMs())
                .build();
    }

    public void start(){
        this.client.start();
    }

    public boolean checkExists(String path) throws Exception {
        checkState();
        Stat stat = this.client.checkExists().forPath(path);
        return stat != null;
    }

    public void setData(String path, byte[] data) throws Exception{
        checkState();
        this.client.setData().forPath(path, data);
    }

    public byte[] getData(String path) throws Exception{
        checkState();
        return this.client.getData().forPath(path);
    }

    public void createEPhemeral(String path) throws Exception {
        checkState();
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
    }

    public void createPersistent(String path) throws Exception {
        checkState();
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
    }

    public void createEPhemeral(String path, byte[] data) throws Exception {
        checkState();
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
    }
    //

    public void createEPhemeral(String path, BackgroundCallback callback) throws Exception {
        checkState();
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground(new org.apache.curator.framework.api.BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                if(callback != null){
                    callback.complete();
                }
            }
        }).forPath(path);
    }


    private void checkState(){
        CuratorFrameworkState state = this.client.getState();
        switch (state){
            case LATENT:
                throw new RuntimeException("ZookeeperClient not start");
            case STARTED:
                return;
            case STOPPED:
                throw new RuntimeException("ZookeeperClient stop");
        }
    }

    public interface BackgroundCallback{

        void complete();
    }


    public void close(){
        this.client.close();
    }
}
