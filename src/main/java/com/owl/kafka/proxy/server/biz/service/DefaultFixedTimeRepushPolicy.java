package com.owl.kafka.proxy.server.biz.service;

import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.MessageCodec;
import com.owl.kafka.proxy.server.biz.bo.ResendPacket;
import com.owl.kafka.proxy.server.biz.bo.ServerConfigs;
import com.owl.kafka.proxy.server.biz.push.PushCenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class DefaultFixedTimeRepushPolicy implements RepushPolicy<Packet>, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFixedTimeRepushPolicy.class);

    private final Thread thread;

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final PushCenter pushCenter;

    private final int reposts = ServerConfigs.I.getServerMessageRepostTimes();

    private final long interval = TimeUnit.SECONDS.toMillis(ServerConfigs.I.getServerMessageRepostInterval());

    public DefaultFixedTimeRepushPolicy(PushCenter pushCenter){
        this.pushCenter = pushCenter;
        this.thread = new Thread(this, "repush-thread");
        this.thread.setDaemon(true);
    }

    public void start(){
        this.start.compareAndSet(false, true);
        this.thread.start();
    }

    public void close(){
        this.start.compareAndSet(true, false);
        this.thread.interrupt();
    }

    @Override
    public void run() {
        while(start.get()){
            try {
                ResendPacket first = MessageHolder.MSG_QUEUE.peek();
                if(first == null){
                    return;
                }
                long now = SystemClock.millisClock().now();
                if(first.getRepost() >= reposts){
                    Message message = MessageCodec.decode(first.getPacket().getBody());
                    MessageHolder.fastRemove(message);
                    LOGGER.warn("packet repost fail ", first);
                    InstanceHolder.I.getDLQService().write(first);
                    continue;
                }
                if(now - first.getTimestamp() >= interval){
                    MessageHolder.MSG_QUEUE.poll();
                    first.setRepost(first.getRepost() + 1);
                    first.setTimestamp(now);
                    try {
                        repush(first.getPacket());
                    } finally{
                        MessageHolder.MSG_QUEUE.offer(first);
                    }
                }
                TimeUnit.MILLISECONDS.sleep(30);
            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException", ex);
            } catch (ChannelInactiveException ex){
                LOGGER.warn("ChannelInactiveException, here can ignore", ex);
            }
        }
    }

    @Override
    public void repush(Packet msg) throws InterruptedException, ChannelInactiveException {
        this.pushCenter.push(msg);
    }

}
