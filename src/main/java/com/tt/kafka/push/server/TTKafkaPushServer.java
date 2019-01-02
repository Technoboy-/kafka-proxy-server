package com.tt.kafka.push.server;

import com.tt.kafka.netty.codec.PacketDecoder;
import com.tt.kafka.netty.codec.PacketEncoder;
import com.tt.kafka.netty.handler.MessageDispatcher;
import com.tt.kafka.netty.protocol.Command;
import com.tt.kafka.push.server.netty.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;

import static com.tt.kafka.util.Constants.*;

/**
 * @Author: Tboy
 */
public class TTKafkaPushServer extends NettyTcpServer {

    private static final int bossNum = Integer.valueOf(System.getProperty("bossNum", "1"));

    private static final int workerNum = Integer.valueOf(System.getProperty("bossNum", String.valueOf(CPU_SIZE)));

    private final PushClientRegistry clientRegistry = new PushClientRegistry();

    private ChannelHandler handler;

    public TTKafkaPushServer(PushServerConfigs configs) {
        super(configs.getPort(), bossNum, workerNum);
        registerHandler();
    }

    private void registerHandler(){
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.LOGIN, new LoginHandler(clientRegistry));
        dispatcher.register(Command.HEARTBEAT, new HeartbeatHandler());
        dispatcher.register(Command.ACK, new AckHandler());
        handler = new PushServerHandler(dispatcher);
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
