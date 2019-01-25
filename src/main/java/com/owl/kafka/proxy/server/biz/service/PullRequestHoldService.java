package com.owl.kafka.proxy.server.biz.service;

import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.proxy.server.biz.bo.PullRequest;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class PullRequestHoldService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullRequestHoldService.class);

    private final ConcurrentHashMap<String, PullRequest> requestHolder = new ConcurrentHashMap<>();

    private final Thread worker;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public PullRequestHoldService(){
        this.start.compareAndSet(false, true);
        this.worker = new Thread(new Runnable() {
            @Override
            public void run() {
                while(start.get()){
                    try {
                        Thread.sleep(5 * 1000);
                        checkRequestHolder();
                    } catch (InterruptedException e) {
                        //Ignore
                    }
                }
            }
        }, "PullRequestHoldService-thread");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public void suspend(PullRequest pullRequest){
        requestHolder.put(pullRequest.getConnection().getId().asLongText(), pullRequest);
    }

    public void close(){
        this.start.compareAndSet(true, false);
        this.worker.interrupt();
        LOGGER.debug("close PullRequestHoldService ");
    }

    public void checkRequestHolder(){
        Iterator<Map.Entry<String, PullRequest>> iterator = requestHolder.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<String, PullRequest> next = iterator.next();
            PullRequest request = next.getValue();
            Packet result = PullCenter.I.pull(request, false);
            boolean execute = executeWhenWakeup(next.getValue(), result);
            if(execute){
                iterator.remove();
            }
        }
    }

    public void notifyMessageArriving(){
        checkRequestHolder();
    }

    private boolean executeWhenWakeup(PullRequest request, Packet result){
        boolean execute = false;
        try {
            if(!result.isBodyEmtpy()){
                request.getConnection().send(result);
                execute = true;
            } else if(System.currentTimeMillis() > (request.getSuspendTimestamp() + request.getTimeoutMs())){
                request.getConnection().send(Packets.pullNoMsgResp(request.getPacket().getOpaque()), new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {

                    }
                });
                execute = true;
            }
        } catch (ChannelInactiveException e) {
            execute = true;
        }
        return execute;
    }
}
