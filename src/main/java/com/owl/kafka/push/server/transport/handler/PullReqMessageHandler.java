package com.owl.kafka.push.server.transport.handler;

import com.owl.kafka.proxy.transport.Connection;
import com.owl.kafka.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.proxy.transport.handler.CommonMessageHandler;
import com.owl.kafka.proxy.transport.protocol.Packet;
import com.owl.kafka.push.server.biz.bo.PullRequest;
import com.owl.kafka.push.server.biz.pull.PullCenter;
import com.owl.kafka.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PullReqMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullReqMessageHandler.class);

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
