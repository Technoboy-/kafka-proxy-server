package com.tt.kafka.push.server.biz;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.client.transport.codec.PacketDecoder;
import com.tt.kafka.client.transport.codec.PacketEncoder;
import com.tt.kafka.client.transport.handler.MessageDispatcher;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.push.server.biz.ServerRegistry;
import com.tt.kafka.push.server.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.push.server.transport.NettyTcpServer;
import com.tt.kafka.push.server.transport.handler.AckMessageHandler;
import com.tt.kafka.push.server.transport.handler.HeartbeatMessageHandler;
import com.tt.kafka.push.server.transport.handler.ServerHandler;
import com.tt.kafka.push.server.transport.handler.UnregisterMessageHandler;
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

    private final ChannelHandler handler;

    private final PushConfigs serverConfigs;

    private final ServerRegistry serverRegistry;

    public NettyServer(PushConfigs configs, DefaultKafkaConsumerImpl consumer) {
        super(configs.getServerPort(), configs.getServerBossNum(), configs.getServerWorkerNum());
        this.serverConfigs = configs;
        this.serverRegistry = new ServerRegistry(configs);
        this.handler = new ServerHandler(newDispatcher(consumer));
    }

    private MessageDispatcher newDispatcher(DefaultKafkaConsumerImpl consumer){
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.HEARTBEAT, new HeartbeatMessageHandler());
        dispatcher.register(Command.UNREGISTER, new UnregisterMessageHandler());
        dispatcher.register(Command.ACK, new AckMessageHandler(consumer));
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
        this.serverRegistry.register();
    }

    protected void initNettyChannel(NioSocketChannel ch) throws Exception{

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("encoder", getEncoder());
        //in
        pipeline.addLast("decoder", getDecoder());
        pipeline.addLast("timeOutHandler", new ReadTimeoutHandler(120));
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
        this.serverRegistry.destroy();
    }

}
