package com.owl.kafka.proxy.server.biz;

import com.owl.kafka.client.proxy.transport.codec.PacketDecoder;
import com.owl.kafka.client.proxy.transport.codec.PacketEncoder;
import com.owl.kafka.client.proxy.transport.handler.MessageDispatcher;
import com.owl.kafka.client.proxy.transport.protocol.Command;
import com.owl.kafka.proxy.server.biz.bo.ServerConfigs;
import com.owl.kafka.proxy.server.biz.service.InstanceHolder;
import com.owl.kafka.proxy.server.consumer.ProxyConsumer;
import com.owl.kafka.proxy.server.transport.NettyTcpServer;
import com.owl.kafka.proxy.server.transport.handler.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;


/**
 * @Author: Tboy
 */
public class NettyServer extends NettyTcpServer {

    private static final int port = ServerConfigs.I.getServerPort();

    private static final int bossNum = ServerConfigs.I.getServerBossNum();

    private static final int workerNum = ServerConfigs.I.getServerWorkerNum();

    private final ChannelHandler handler;

    public NettyServer(ProxyConsumer consumer) {
        super(port, bossNum, workerNum);
        this.handler = new ServerHandler(newDispatcher(consumer));
    }

    private MessageDispatcher newDispatcher(ProxyConsumer consumer){
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.PING, new PingMessageHandler());
        dispatcher.register(Command.UNREGISTER, new UnregisterMessageHandler());
        dispatcher.register(Command.ACK, new AckMessageHandler(consumer));
        dispatcher.register(Command.VIEW_REQ, new ViewReqMessageHandler());
        dispatcher.register(Command.PULL_REQ, new PullReqMessageHandler());
        dispatcher.register(Command.SEND_BACK, new SendBackMessageHandler());
        return dispatcher;
    }

    protected void initTcpOptions(ServerBootstrap bootstrap){
        super.initTcpOptions(bootstrap);
        bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_SNDBUF, 64 * 1024) //64k
                .option(ChannelOption.SO_RCVBUF, 64 * 1024); //64k
    }

    @Override
    protected void afterStart() {
        InstanceHolder.I.getRegistryCenter().getServerRegistry().register();
    }

    protected void initNettyChannel(NioSocketChannel ch) throws Exception{

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("encoder", getEncoder());
        //in
        pipeline.addLast("decoder", getDecoder());
        pipeline.addLast("timeOutHandler", new ReadTimeoutHandler(90));
        pipeline.addLast("handler", getChannelHandler());
    }

    @Override
    protected ChannelHandler getEncoder() {
        return new PacketEncoder();
    }

    @Override
    protected ChannelHandler getDecoder() {
        return new PacketDecoder();
    }

    @Override
    protected ChannelHandler getChannelHandler() {
        return handler;
    }

    public void close(){
        super.close();
    }

}
