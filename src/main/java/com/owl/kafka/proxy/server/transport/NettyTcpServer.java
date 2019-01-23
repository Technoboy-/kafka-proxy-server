package com.owl.kafka.proxy.server.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @Author: Tboy
 */
public abstract class NettyTcpServer  {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpServer.class);

    private final ServerBootstrap bootstrap;

    private final EventLoopGroup bossGroup;

    private final EventLoopGroup workGroup;

    private final int port;

    private final int bossNum;

    private final int workerNum;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public NettyTcpServer(int port, int bossNum, int workerNum){
        this.port = port;
        this.bossNum = bossNum;
        this.workerNum = workerNum;
        bootstrap = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup(this.getBossNum());
        workGroup = new NioEventLoopGroup(this.getWorkerNum());
        initTcpOptions(bootstrap);
    }

    public void start(){
        if(start.compareAndSet(false, true)){
            initNettyChannel();
            ChannelFuture cf = null;
            try {
                cf = bootstrap.bind(port).sync();
            } catch (InterruptedException e) {
                LOGGER.error("NettyTcpServer bind fail {}", e);
            }
            if (cf.isSuccess()) {
                LOGGER.info("NettyTcpServer bind success at port : {}", port);
            } else if (cf.cause() != null) {
                LOGGER.error("NettyTcpServer bind fail {}", cf.cause());
            } else {
                LOGGER.error("NettyTcpServer bind fail, exit");
                System.exit(1);
            }
            afterStart();
        }
    }

    protected abstract void afterStart();

    private void initNettyChannel(){
        bootstrap.group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {

                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        initNettyChannel(ch);
                    }
                });
    }

    protected void initNettyChannel(NioSocketChannel ch) throws Exception{
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("encoder", getEncoder());
        pipeline.addLast("decoder", getDecoder());
        pipeline.addLast("handler", getChannelHandler());
    }

    protected void initTcpOptions(ServerBootstrap bootstrap){
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true);
    }

    protected abstract ChannelHandler getEncoder();

    protected abstract ChannelHandler getDecoder();

    protected abstract ChannelHandler getChannelHandler();

    public int getBossNum() {
        return bossNum;
    }

    public int getWorkerNum() {
        return workerNum;
    }

    public void close() {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            LOGGER.info("NettyTcpServer has closed boss group.");
        }
        if (workGroup != null) {
            workGroup.shutdownGracefully();
            LOGGER.info("NettyTcpServer has closed work group.");
        }
    }
}
