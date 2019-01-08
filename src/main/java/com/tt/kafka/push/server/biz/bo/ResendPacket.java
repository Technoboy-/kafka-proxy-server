package com.tt.kafka.push.server.biz.bo;

import com.tt.kafka.client.transport.protocol.Packet;

import java.util.Objects;

/**
 * @Author: Tboy
 */
public class ResendPacket implements Comparable<ResendPacket> {

    private long msgId;

    private long timestamp;

    private Packet packet;

    public ResendPacket(long msgId){
        this.msgId = msgId;
    }

    public ResendPacket(Packet packet){
        this.msgId = packet.getMsgId();
        this.timestamp = System.currentTimeMillis();
        this.packet = packet;
    }

    public long getMsgId() {
        return msgId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Packet getPacket() {
        return packet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResendPacket that = (ResendPacket) o;
        return getMsgId() == that.getMsgId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMsgId());
    }

    @Override
    public int compareTo(ResendPacket o) {
        return this.timestamp < o.timestamp ? -1 : (this.timestamp > o.timestamp ? 1 : 0);
    }
}
