package com.owl.kafka.proxy.server.biz.service;

import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.client.util.NamedThreadFactory;
import com.owl.kafka.proxy.server.biz.bo.PullRequest;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class EnhancedPullRequestHoldService implements TimerTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnhancedPullRequestHoldService.class);

    private final ConcurrentHashMap<String, PullRequest> requestHolder = new ConcurrentHashMap<>();

    private final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("PullRequestHoldService"));

    private volatile long lastNotifyTimeMs = System.currentTimeMillis();

    private volatile long lastCheckTimeMs = System.currentTimeMillis();

    private final long delay = 3000;

    private final long lastNotifyPeriod = 1;

    private final long lastCheckPeriod = 1;

    public EnhancedPullRequestHoldService(){
        timer.newTimeout(this, delay, TimeUnit.MILLISECONDS);
    }

    public void suspend(PullRequest pullRequest){
        requestHolder.put(pullRequest.getConnection().getId().asLongText(), pullRequest);
    }

    public void close(){
        this.timer.stop();
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
        if(System.currentTimeMillis() - lastNotifyTimeMs > lastNotifyPeriod){
            lastNotifyTimeMs = System.currentTimeMillis();
            timer.newTimeout(this, 0, TimeUnit.MILLISECONDS);
        }
    }

    private boolean executeWhenWakeup(PullRequest request, Packet result){
        boolean execute = false;
        try {
            if(!result.isBodyEmtpy()){
                request.getConnection().send(result);
                execute = true;
            } else if(System.currentTimeMillis() > (request.getSuspendTimestamp() + request.getTimeoutMs())){
                final Packet packet = Packets.pullNoMsgResp(request.getPacket().getOpaque());
                request.getConnection().send(packet);
                execute = true;
            }
        } catch (ChannelInactiveException e) {
            LOGGER.warn("ChannelInactiveException, ignore", e);
            execute = true;
        }
        return execute;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if(System.currentTimeMillis() - lastCheckTimeMs > lastCheckPeriod){
            lastCheckTimeMs = System.currentTimeMillis();
            checkRequestHolder();
        }
        timer.newTimeout(this, delay, TimeUnit.MILLISECONDS);
    }
}
