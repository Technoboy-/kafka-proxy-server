package com.tt.kafka.push.server.biz.service;

import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.push.server.biz.bo.FastResendMessage;
import com.tt.kafka.push.server.biz.bo.ResendPacket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class MessageHolder {

    public static MessageHolder I = new MessageHolder();

    private static final ConcurrentHashMap<Long, FastResendMessage> MSG_MAPPER = new ConcurrentHashMap(1000);

    public static final PriorityBlockingQueue<ResendPacket> MSG_QUEUE = new PriorityBlockingQueue<>(1000);

    private static final AtomicLong MEMORY_SIZE = new AtomicLong(0);

    private static final AtomicLong COUNT = new AtomicLong(0);

    public static long memorySize(){
        return MEMORY_SIZE.get();
    }

    public static long count(){
        return COUNT.get();
    }

    public static void fastPut(Packet packet){
        if(packet == null){
            return;
        }
        MSG_QUEUE.put(new ResendPacket(packet));
        long size = packet.getHeader().length + packet.getKey().length + packet.getValue().length;
        MSG_MAPPER.put(packet.getMsgId(), new FastResendMessage(packet.getMsgId(), packet.getHeader(), size));
        COUNT.incrementAndGet();
        MEMORY_SIZE.addAndGet(size);
    }

    public static Packet fastRemove(Packet packet){
        MSG_QUEUE.remove(new ResendPacket(packet.getMsgId()));
        FastResendMessage frm = MSG_MAPPER.remove(packet.getMsgId());
        if(frm != null){
            packet.setHeader(frm.getHeader());
            COUNT.decrementAndGet();
            MEMORY_SIZE.addAndGet(frm.getSize()*(-1));
        }
        return packet;
    }
}
