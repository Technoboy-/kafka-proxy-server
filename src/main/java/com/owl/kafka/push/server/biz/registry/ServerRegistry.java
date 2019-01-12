package com.owl.kafka.push.server.biz.registry;

import com.owl.kafka.client.service.RegisterMetadata;
import com.owl.kafka.client.service.RegistryService;
import com.owl.kafka.client.transport.Address;
import com.owl.kafka.util.NamedThreadFactory;
import com.owl.kafka.util.NetUtils;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;

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

    public ServerRegistry(RegistryService registryService){
        this.registryService = registryService;
        this.executorService = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("register-zk-thread"));
    }

    public void register(){
        Address address = new Address(NetUtils.getLocalIp(), ServerConfigs.I.getServerPort());
        RegisterMetadata registerMetadata = new RegisterMetadata();
        registerMetadata.setPath(String.format(ServerConfigs.I.ZOOKEEPER_PROVIDERS, ServerConfigs.I.getServerTopic()));
        registerMetadata.setAddress(address);
        this.registryService.register(registerMetadata);
        startSchedulerTask();
    }

    public void unregister(){
        Address address = new Address(NetUtils.getLocalIp(), ServerConfigs.I.getServerPort());
        RegisterMetadata registerMetadata = new RegisterMetadata();
        registerMetadata.setPath(String.format(ServerConfigs.I.ZOOKEEPER_PROVIDERS, ServerConfigs.I.getServerTopic()));
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
