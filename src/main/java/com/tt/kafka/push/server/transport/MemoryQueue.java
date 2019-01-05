package com.tt.kafka.push.server.transport;

import com.tt.kafka.client.transport.protocol.Packet;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Author: jiwei.guo
 * @Date: 2019/1/5 7:32 PM
 */
public class MemoryQueue {

    public static final LinkedBlockingQueue<Packet> ackQueue = new LinkedBlockingQueue(1000);
}
