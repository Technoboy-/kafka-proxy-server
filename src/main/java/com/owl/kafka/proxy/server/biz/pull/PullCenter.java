package com.owl.kafka.proxy.server.biz.pull;

import com.owl.kafka.client.proxy.service.IdService;
import com.owl.kafka.client.proxy.service.PullStatus;
import com.owl.kafka.client.proxy.transport.message.Header;
import com.owl.kafka.client.proxy.transport.protocol.Command;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.serializer.SerializerImpl;
import com.owl.kafka.proxy.server.biz.bo.PullRequest;
import com.owl.kafka.proxy.server.biz.bo.ServerConfigs;
import com.owl.kafka.proxy.server.biz.service.PullRequestHoldService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @Author: Tboy
 */
public class PullCenter{

    private static final Logger LOGGER = LoggerFactory.getLogger(PullCenter.class);

    public static PullCenter I  = new PullCenter();

    private final int queueSize = ServerConfigs.I.getServerQueueSize();

    private final ArrayBlockingQueue<Packet> retryQueue = new ArrayBlockingQueue<>(queueSize);

    private final ArrayBlockingQueue<ConsumerRecord<byte[], byte[]>> pullQueue = new ArrayBlockingQueue<>(queueSize);

    private final int pullMessageCount = ServerConfigs.I.getServerPullMessageCount();

    private final long messageSize = ServerConfigs.I.getServerPullMessageSize();

    private final PullRequestHoldService pullRequestHoldService = new PullRequestHoldService();

    private final ByteBufAllocator allocator = new PooledByteBufAllocator(true);

    public void putMessage(ConsumerRecord<byte[], byte[]> record) throws InterruptedException{
        this.pullQueue.put(record);
        this.pullRequestHoldService.notifyMessageArriving();
    }

    public void reputMessage(Packet packet) throws InterruptedException{
        this.retryQueue.put(packet);
    }

    public Packet pull(PullRequest request, boolean isSuspend) {
        long messageCount = pullMessageCount;
        final Packet result = request.getPacket();
        result.setCmd(Command.PULL_RESP.getCmd());
        while(messageCount > 0 && result.getBodyLength() < messageSize * pullMessageCount){
            messageCount--;
            if(!this.poll(result)){
                break;
            }
        }
        if(result.isBodyEmtpy() && isSuspend){
            pullRequestHoldService.suspend(request);
        }
        return result;
    }

    private boolean poll(Packet packet) {
        boolean polled = false;
        Packet one = retryQueue.peek();
        if(one != null){
            retryQueue.poll();
            ByteBuf buffer = allocator.directBuffer(one.getBody().length + packet.getBody().length);
            try {
                buffer.writeBytes(packet.getBody());
                buffer.writeBytes(one.getBody());
                packet.setBody(buffer.array());
            } finally {
                buffer.release();
            }
            polled = true;
        } else{
            ConsumerRecord<byte[], byte[]> record = pullQueue.poll();
            if(record != null){
                Header header = new Header(record.topic(), record.partition(), record.offset(),
                        IdService.I.getId(), PullStatus.FOUND.getStatus());
                byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
                //
                int capacity = 4 + headerInBytes.length + 4 + record.key().length + 4 + record.value().length;
                ByteBuf buffer = allocator.directBuffer(capacity + packet.getBody().length);
                try {
                    buffer.writeBytes(packet.getBody());
                    buffer.writeInt(headerInBytes.length);
                    buffer.writeBytes(headerInBytes);
                    buffer.writeInt(record.key().length);
                    buffer.writeBytes(record.key());
                    buffer.writeInt(record.value().length);
                    buffer.writeBytes(record.value());
                    //
                    packet.setBody(buffer.array());
                } finally {
                    buffer.release();
                }
                polled = true;
            }
        }
        return polled;
    }

    public void close(){
        this.pullRequestHoldService.close();
    }
}
