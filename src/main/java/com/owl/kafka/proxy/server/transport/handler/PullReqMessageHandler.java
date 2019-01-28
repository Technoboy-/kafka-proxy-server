package com.owl.kafka.proxy.server.transport.handler;

import com.owl.kafka.client.proxy.transport.alloc.ByteBufferPool;
import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.util.NetUtils;
import com.owl.kafka.proxy.server.biz.bo.PullRequest;
import com.owl.kafka.proxy.server.biz.pull.PullCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PullReqMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullReqMessageHandler.class);

    private final ByteBufferPool bufferPool = ByteBufferPool.DEFAULT;

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received pull request : {}, from : {}", packet, NetUtils.getRemoteAddress(connection.getChannel()));
        }
        final boolean isSuspend = true;
        PullRequest pullRequest = new PullRequest(connection, packet, 15 * 1000);
        Packet result = PullCenter.I.pull(pullRequest, isSuspend);
        //
        if(!result.isBodyEmtpy()){
            try {
                connection.send(result);
            } catch (ChannelInactiveException ex){
                PullCenter.I.reputMessage(result);
            }
        }
    }

}
