package com.owl.kafka.push.server.biz.service;

/**
 * @Author: Tboy
 */
public class InstanceHolder {

    public static InstanceHolder I = new InstanceHolder();

    private DLQService dlqService;

    public void setDLQService(DLQService service){
        this.dlqService = service;
    }

    public DLQService getDLQService(){
        return this.dlqService;
    }

}
