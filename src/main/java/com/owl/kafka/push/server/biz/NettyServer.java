package com.owl.kafka.push.server.biz;

import com.owl.kafka.client.transport.codec.PacketDecoder;
import com.owl.kafka.client.transport.codec.PacketEncoder;
import com.owl.kafka.client.transport.handler.MessageDispatcher;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.push.server.biz.registry.RegistryCenter;
import com.owl.kafka.push.server.biz.service.InstanceHolder;
import com.owl.kafka.push.server.transport.NettyTcpServer;
import com.owl.kafka.push.server.transport.handler.*;
import com.owl.kafka.client.transport.protocol.Command;
import com.owl.kafka.push.server.consumer.PushServerConsumer;
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

    public NettyServer(PushServerConsumer consumer) {
        super(port, bossNum, workerNum);
        this.handler = new ServerHandler(newDispatcher(consumer));
    }

    private MessageDispatcher newDispatcher(PushServerConsumer consumer){
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.PING, new HeartbeatMessageHandler());
        dispatcher.register(Command.UNREGISTER, new UnregisterMessageHandler());
        dispatcher.register(Command.ACK, new AckMessageHandler(consumer));
        dispatcher.register(Command.VIEW, new ViewMessageHandler());
        dispatcher.register(Command.PULL, new PullMessageHandler());
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
        RegistryCenter.I.getServerRegistry().register();
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
