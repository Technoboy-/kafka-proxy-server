package com.owl.kafka.proxy.server.biz.service;

import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.client.util.CollectionUtils;
import com.owl.kafka.client.util.NamedThreadFactory;
import com.owl.kafka.proxy.server.biz.bo.ManyPullRequest;
import com.owl.kafka.proxy.server.biz.bo.PullRequest;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class EnhancedPullRequestHoldService implements TimerTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnhancedPullRequestHoldService.class);

    private final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("PullRequestHoldService"));

    private final long delay = 3000;

    private final ManyPullRequest manyPullRequest = new ManyPullRequest();

    public EnhancedPullRequestHoldService(){
        timer.newTimeout(this, delay, TimeUnit.MILLISECONDS);
    }

    public void suspend(PullRequest pullRequest){
        this.manyPullRequest.add(pullRequest);
    }

    public void close(){
        this.timer.stop();
        LOGGER.debug("close PullRequestHoldService ");
    }

    public void checkRequestHolder(){
        long start = System.currentTimeMillis();
        List<PullRequest> pullRequests = this.manyPullRequest.cloneAndClear();
        if(pullRequests != null){
            ArrayList<PullRequest> notExecuted = new ArrayList<>(pullRequests.size());
            for(PullRequest pullRequest : pullRequests){
                Packet result = PullCenter.I.pull(pullRequest, false);
                boolean execute = executeWhenWakeup(pullRequest, result);
                if(!execute){
                    notExecuted.add(pullRequest);
                }
            }
            if(!CollectionUtils.isEmpty(notExecuted)){
                this.manyPullRequest.add(notExecuted);
            }
        }
        long takes = System.currentTimeMillis() - start;
        if(takes > 1){
            LOGGER.error("checkRequestHolder() takes {} ms", takes);
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
        checkRequestHolder();
        this.timer.newTimeout(this, delay, TimeUnit.MILLISECONDS);
    }
}
