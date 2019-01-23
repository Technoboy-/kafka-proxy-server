package com.owl.kafka.proxy.server.biz.bo;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: Tboy
 */
public class FastResendMessage implements Serializable {

    private final long msgId;

    private final byte[] header;

    private final long size;

    private final long timestamp;

    public FastResendMessage(long msgId, byte[] header, long size){
        this.msgId = msgId;
        this.header = header;
        this.size = size;
        this.timestamp = System.currentTimeMillis();
    }

    public long getMsgId() {
        return msgId;
    }

    public long getSize() {
        return size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getHeader() {
        return header;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FastResendMessage that = (FastResendMessage) o;
        return getMsgId() == that.getMsgId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMsgId());
    }

    @Override
    public String toString() {
        return "FastResendMessage{" +
                "msgId=" + msgId +
                ", size=" + size +
                ", timestamp=" + timestamp +
                '}';
    }
}
