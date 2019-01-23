package com.owl.kafka.proxy.server.biz.service;

import com.owl.kafka.proxy.server.biz.bo.FastResendMessage;
import com.owl.kafka.proxy.server.biz.bo.ResendPacket;
import com.owl.kafka.proxy.transport.message.Message;
import com.owl.kafka.proxy.transport.protocol.Packet;
import com.owl.kafka.proxy.util.MessageCodec;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static void fastPut(Packet packet){
        if(packet == null){
            return;
        }
        lock.writeLock().lock();
        try {
            Message message = MessageCodec.decode(packet.getBody());
            MSG_QUEUE.put(new ResendPacket(message.getHeader().getMsgId()));
            long size = message.getHeaderInBytes().length + message.getKey().length + message.getValue().length;
            MSG_MAPPER.put(message.getHeader().getMsgId(), new FastResendMessage(message.getHeader().getMsgId(), message.getHeaderInBytes(), size));
            COUNT.incrementAndGet();
            MEMORY_SIZE.addAndGet(size);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static Message fastRemove(Packet packet){
        Message message = null;
        lock.writeLock().lock();
        try {
            message = MessageCodec.decode(packet.getBody());
            MSG_QUEUE.remove(new ResendPacket(message.getHeader().getMsgId()));
            FastResendMessage frm = MSG_MAPPER.remove(message.getHeader().getMsgId());
            if(frm != null){
                COUNT.decrementAndGet();
                MEMORY_SIZE.addAndGet(frm.getSize()*(-1));
            }
            return message;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
