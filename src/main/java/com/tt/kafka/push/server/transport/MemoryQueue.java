package com.tt.kafka.push.server.transport;

import com.tt.kafka.client.transport.protocol.Packet;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: jiwei.guo
 * @Date: 2019/1/5 7:32 PM
 */
public class MemoryQueue {

    public static MemoryQueue I = new MemoryQueue();

    public static final ConcurrentHashMap<Long, Packet> ackMap = new ConcurrentHashMap(1000);

}
