package com.owl.kafka.proxy.server.biz.bo;


import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.protocol.Packet;

import java.util.Objects;

/**
 * @Author: Tboy
 */
public class PullRequest {

    private Connection connection;

    private Packet packet;

    private long suspendTimestamp;

    private long timeoutMs;

    public PullRequest(Connection connection, Packet packet, long timeoutMs){
        this.connection = connection;
        this.packet = packet;
        this.suspendTimestamp = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Packet getPacket() {
        return packet;
    }

    public void setPacket(Packet packet) {
        this.packet = packet;
    }


    public long getSuspendTimestamp() {
        return suspendTimestamp;
    }

    public void setSuspendTimestamp(long suspendTimestamp) {
        this.suspendTimestamp = suspendTimestamp;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PullRequest that = (PullRequest) o;
        return Objects.equals(packet.getOpaque(), that.packet.getOpaque());
    }

    @Override
    public int hashCode() {
        return Objects.hash(packet.getOpaque());
    }
}
