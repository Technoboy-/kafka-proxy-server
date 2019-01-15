package com.owl.kafka.push.server.biz.service;

import com.owl.kafka.client.service.RegistryService;

/**
 * @Author: Tboy
 */
public class InstanceHolder {

    public static InstanceHolder I = new InstanceHolder();

    private RegistryService registryService;

    private DLQService dlqService;

    public void setDLQService(DLQService service){
        this.dlqService = service;
    }

    public DLQService getDLQService(){
        return this.dlqService;
    }


    public RegistryService getRegistryService() {
        return registryService;
    }

    public void setRegistryService(RegistryService registryService) {
        this.registryService = registryService;
    }
}
