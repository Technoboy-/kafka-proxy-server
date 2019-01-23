package com.owl.kafka.proxy.server.biz.bo;

import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.proxy.server.biz.service.SystemClock;

import java.util.Objects;

/**
 * @Author: Tboy
 */
public class ResendPacket implements Comparable<ResendPacket> {

    private long msgId;

    private int repost;

    private long timestamp;

    private Packet packet;

    public ResendPacket(long msgId){
        this.msgId = msgId;
    }

    public ResendPacket(long msgId, Packet packet){
        this.msgId = msgId;
        this.repost = 1;
        this.timestamp = SystemClock.millisClock().now();
        this.packet = packet;
    }

    public int getRepost() {
        return repost;
    }

    public void setRepost(int repost) {
        this.repost = repost;
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
    public String toString() {
        return "ResendPacket{" +
                "msgId=" + msgId +
                ", repost=" + repost +
                ", timestamp=" + timestamp +
                ", packet=" + packet +
                '}';
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
