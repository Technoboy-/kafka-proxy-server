package com.tt.kafka.push.server.netty;
import com.tt.kafka.netty.handler.MessageDispatcher;
import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.transport.Connection;
import com.tt.kafka.netty.transport.NettyConnection;
import com.tt.kafka.util.NetUtils;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
@Sharable
public class PushServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServerHandler.class);

    private final MessageDispatcher dispatcher;

    public PushServerHandler(MessageDispatcher dispatcher){
        this.dispatcher = dispatcher;
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        NettyConnection.attachChannel(ctx.channel());
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Connection connnection = NettyConnection.attachChannel(ctx.channel());
        connnection.close();
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        dispatcher.dispatch(NettyConnection.attachChannel(ctx.channel()), (Packet)msg);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Connection connnection = NettyConnection.attachChannel(ctx.channel());
        LOGGER.error("clientId : {} get exception {} , close channel [ip:{}]",  new Object[]{connnection, cause, NetUtils.getRemoteAddress(ctx.channel())});
        ctx.close();
    }
}
