package com.tt.kafka.push.server.biz;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.client.service.RegisterMetadata;
import com.tt.kafka.client.service.RegistryService;
import com.tt.kafka.client.transport.Address;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.NamedThreadFactory;
import com.tt.kafka.util.NetUtils;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class ServerRegistry {

    private final RegistryService registryService;

    private final ScheduledThreadPoolExecutor executorService;

    private ScheduledFuture<?> registerScheduledFuture;

    private final PushConfigs serverConfigs;

    public ServerRegistry(PushConfigs configs){
        this.serverConfigs = configs;
        this.registryService = new RegistryService(configs);
        this.executorService = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("register-zk-thread"));
    }

    public void register(){
        Address address = new Address(NetUtils.getLocalIp(), serverConfigs.getServerPort());
        RegisterMetadata registerMetadata = new RegisterMetadata();
        registerMetadata.setPath(String.format(Constants.ZOOKEEPER_PROVIDERS, serverConfigs.getServerTopic()));
        registerMetadata.setAddress(address);
        this.registryService.register(registerMetadata);
        startSchedulerTask();
    }

    public void unregister(){
        Address address = new Address(NetUtils.getLocalIp(), serverConfigs.getServerPort());
        RegisterMetadata registerMetadata = new RegisterMetadata();
        registerMetadata.setPath(String.format(Constants.ZOOKEEPER_PROVIDERS, serverConfigs.getServerTopic()));
        registerMetadata.setAddress(address);
        this.registryService.unregister(registerMetadata);
    }

    private void startSchedulerTask(){
        registerScheduledFuture = executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                register();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public void destroy(){
        if(registerScheduledFuture != null && !registerScheduledFuture.isDone()){
            registerScheduledFuture.cancel(true);
        }
        this.executorService.purge();
        this.unregister();
    }
}
