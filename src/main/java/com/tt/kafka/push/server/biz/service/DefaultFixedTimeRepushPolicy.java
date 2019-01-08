package com.tt.kafka.push.server.biz.service;

import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.push.server.biz.PushCenter;
import com.tt.kafka.push.server.biz.bo.ResendPacket;
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
                long now = System.currentTimeMillis();
                if(now - first.getTimestamp() >= 3 * 1000){
                    MessageHolder.MSG_QUEUE.poll();
                    first.setTimestamp(now);
                    try {
                        repush(first.getPacket());
                    } finally{
                        MessageHolder.MSG_QUEUE.offer(first);
                    }
                }
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException", ex);
            }
        }
    }

    @Override
    public void repush(Packet msg) throws InterruptedException{
        this.pushCenter.push(msg);
    }
}
