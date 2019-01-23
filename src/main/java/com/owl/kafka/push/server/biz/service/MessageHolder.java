package com.owl.kafka.push.server.biz.service;

import com.owl.kafka.client.transport.message.Message;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.MessageCodec;
import com.owl.kafka.push.server.biz.bo.FastResendMessage;
import com.owl.kafka.push.server.biz.bo.ResendPacket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
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

    private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public static void fastPut(Packet packet){
        if(packet == null){
            return;
        }
        final Lock writeLock = readWriteLock.writeLock();
        try {
            writeLock.lock();
            Message message = MessageCodec.decode(packet.getBody());
            MSG_QUEUE.put(new ResendPacket(message.getHeader().getMsgId()));
            long size = message.getHeaderInBytes().length + message.getKey().length + message.getValue().length;
            MSG_MAPPER.put(message.getHeader().getMsgId(), new FastResendMessage(message.getHeader().getMsgId(), message.getHeaderInBytes(), size));
            COUNT.incrementAndGet();
            MEMORY_SIZE.addAndGet(size);
        } finally {
            writeLock.unlock();
        }
    }

    public static Message fastRemove(Packet packet){
        Message message = null;
        final Lock writeLock = readWriteLock.writeLock();
        try {
            writeLock.lock();
            message = MessageCodec.decode(packet.getBody());
            MSG_QUEUE.remove(new ResendPacket(message.getHeader().getMsgId()));
            FastResendMessage frm = MSG_MAPPER.remove(message.getHeader().getMsgId());
            if(frm != null){
                COUNT.decrementAndGet();
                MEMORY_SIZE.addAndGet(frm.getSize()*(-1));
            }
            return message;
        } finally {
            writeLock.unlock();
        }
    }
}
