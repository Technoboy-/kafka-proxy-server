package com.owl.kafka.proxy.server.transport.handler;

import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.NettyConnection;
import com.owl.kafka.client.proxy.transport.handler.MessageDispatcher;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.proxy.server.biz.service.InstanceHolder;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
@Sharable
public class ServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHandler.class);

    private final MessageDispatcher dispatcher;

    public ServerHandler(MessageDispatcher dispatcher){
        this.dispatcher = dispatcher;
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        NettyConnection.attachChannel(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Connection connnection = NettyConnection.attachChannel(ctx.channel());
        InstanceHolder.I.getRegistryCenter().getClientRegistry().unregister(connnection);
        connnection.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        dispatcher.dispatch(NettyConnection.attachChannel(ctx.channel()), (Packet)msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error("exceptionCaught", cause);
        ctx.close();
    }


}
