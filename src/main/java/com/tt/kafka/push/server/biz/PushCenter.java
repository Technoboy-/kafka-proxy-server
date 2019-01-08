package com.tt.kafka.push.server.biz;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.client.service.DefaultRetryPolicy;
import com.tt.kafka.client.service.IdService;
import com.tt.kafka.client.service.LoadBalance;
import com.tt.kafka.client.service.RetryPolicy;
import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Header;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.push.server.transport.MemoryQueue;
import com.tt.kafka.serializer.SerializerImpl;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class PushCenter implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(PushCenter.class);

    private final LoadBalance<Connection> loadBalance;

    private final RetryPolicy retryPolicy;

    private final Thread worker;

    private final LinkedBlockingQueue<Packet> retryQueue;

    private final LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>> pushQueue;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public PushCenter(PushConfigs serverConfigs){
        this.loadBalance = new RoundRobinLoadBalance();
        this.retryPolicy = new DefaultRetryPolicy();
        this.retryQueue = new LinkedBlockingQueue<>(serverConfigs.getServerQueueSize());
        this.pushQueue = new LinkedBlockingQueue<>(serverConfigs.getServerQueueSize());
        this.start.compareAndSet(false, true);
        this.worker = new Thread(this,"push-worker");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public void push(Packet packet, final ChannelFutureListener listener) throws InterruptedException{
        retryPolicy.reset();
        Connection connection = loadBalance.select(ClientRegistry.I.getCopyClients());
        while((connection == null && retryPolicy.allowRetry()) || (!connection.isWritable())){
            connection = loadBalance.select(ClientRegistry.I.getCopyClients());
        }
        //
        connection.send(packet, listener);
    }

    @Override
    public void run() {
        while(this.start.get()){
            Packet ref = null;
            try {
                final Packet packet = take();
                ref = packet;
                this.push(packet, new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if(future.isSuccess()){
                            MemoryQueue.ackMap.put(packet.getMsgId(), packet);
                        } else {
                            retryQueue.put(packet);
                        }
                    }
                });
            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException", ex);
            } catch (Exception ex){
                LOGGER.error("Exception", ex);
                //in case of NettyConnection throw RuntimeException to miss packet
                if(ref != null){
                    try {
                        retryQueue.put(ref);
                    } catch (InterruptedException e) {
                        LOGGER.error("inner InterruptedException", ex);
                    }
                }
            }
        }
    }

    private Packet take() throws InterruptedException{
        Packet packet = retryQueue.peek();
        if(packet != null){
            retryQueue.poll();
        } else{
            ConsumerRecord<byte[], byte[]> record = pushQueue.take();
            packet = new Packet();
            //
            packet.setCmd(Command.PUSH.getCmd());
            packet.setMsgId(IdService.I.getId());
            Header header = new Header(record.topic(), record.partition(), record.offset());
            packet.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
            packet.setKey(record.key());
            packet.setValue(record.value());
            //
        }
        return packet;
    }

    public LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>> getPushQueue() {
        return pushQueue;
    }

    public void close() {
        this.start.compareAndSet(true, false);
        this.worker.interrupt();
    }
}
