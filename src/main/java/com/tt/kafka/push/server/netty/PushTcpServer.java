package com.tt.kafka.push.server.netty;

import com.tt.kafka.netty.codec.PacketDecoder;
import com.tt.kafka.netty.codec.PacketEncoder;
import com.tt.kafka.netty.handler.MessageDispatcher;
import com.tt.kafka.netty.protocol.Command;
import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.transport.Connection;
import com.tt.kafka.push.server.PushServerConfigs;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;


/**
 * @Author: Tboy
 */
public class PushTcpServer extends NettyTcpServer {

    private final PushClientRegistry clientRegistry;

    private final LoadBalancePolicy loadBalancePolicy;

    private final ChannelHandler handler;

    public PushTcpServer(PushServerConfigs configs) {
        super(configs.getPort(), configs.getBossNum(), configs.getWorkerNum());
        this.clientRegistry = new PushClientRegistry();
        this.loadBalancePolicy = new RoundRobinPolicy(clientRegistry);
        this.handler = new PushServerHandler(newDispatcher());
    }

    private MessageDispatcher newDispatcher(){
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.LOGIN, new LoginHandler(clientRegistry));
        dispatcher.register(Command.HEARTBEAT, new HeartbeatHandler());
        dispatcher.register(Command.ACK, new AckHandler());
        return dispatcher;
    }

    protected void initTcpOptions(ServerBootstrap bootstrap){
        super.initTcpOptions(bootstrap);
        bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_SNDBUF, 64 * 1024) //64k
                .option(ChannelOption.SO_RCVBUF, 64 * 1024); //64k
    }

    protected void initNettyChannel(NioSocketChannel ch) throws Exception{

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("encoder", getEncoder());
        //in
        pipeline.addLast("decoder", getDecoder());
        pipeline.addLast("timeOutHandler", new ReadTimeoutHandler(120));
        pipeline.addLast("handler", getChannelHandler());
    }

    public void push(Packet packet){
        Connection connection = loadBalancePolicy.getConnection();
        if(connection != null){
            connection.send(packet);
        }
    }

    public PushClientRegistry getClientRegistry() {
        return clientRegistry;
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
}
