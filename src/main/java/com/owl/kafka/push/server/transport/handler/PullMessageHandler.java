package com.owl.kafka.push.server.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.transport.handler.CommonMessageHandler;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.push.server.biz.bo.PullRequest;
import com.owl.kafka.push.server.biz.pull.PullCenter;
import com.owl.kafka.util.CollectionUtils;
import com.owl.kafka.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author: Tboy
 */
public class PullMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullMessageHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received pull request : {}, from : {}", packet, NetUtils.getRemoteAddress(connection.getChannel()));
        }
        final boolean isSuspend = true;
        PullRequest pullRequest = new PullRequest(connection, packet, 15 * 1000);
        List<Packet> records = PullCenter.I.pull(pullRequest, isSuspend);
        //
        if(!CollectionUtils.isEmpty(records)){
            for(Packet record : records){
                try {
                    record.setOpaque(packet.getOpaque());
                    connection.send(record);
                } catch (ChannelInactiveException ex){
                    PullCenter.I.reputMessage(record);
                }
            }
        }
    }

}
