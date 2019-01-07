package com.tt.kafka.push.server.consumer;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.consumer.service.RebalanceMessageListenerService;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Header;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.service.IdService;
import com.tt.kafka.push.server.boostrap.PushTcpServer;
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
public class PushServerMessageListenerService<K, V> extends RebalanceMessageListenerService<K, V> implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServerMessageListenerService.class);

    private final LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>> pushQueue;

    private final LinkedBlockingQueue<Packet> retryQueue;

    private final PushTcpServer pushTcpServer;

    private final Thread worker;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public PushServerMessageListenerService(PushTcpServer pushTcpServer, PushConfigs serverConfigs){
        this.pushQueue = new LinkedBlockingQueue<>(serverConfigs.getServerQueueSize());
        this.retryQueue = new LinkedBlockingQueue<>(serverConfigs.getServerQueueSize());
        this.pushTcpServer = pushTcpServer;
        this.start.compareAndSet(false, true);
        this.worker = new Thread(this,"push-worker");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        try {
            pushQueue.put(record);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void run() {
        while(this.start.get()){
            try {
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
                final Packet one = packet;
                pushTcpServer.push(one, new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if(future.isSuccess()){
                            MemoryQueue.ackMap.put(one.getMsgId(), one);
                        } else {
                            retryQueue.put(one);
                        }
                    }
                });
            } catch (InterruptedException ex) {
                LOGGER.error("InterruptedException", ex);
            }
        }
    }

    @Override
    public void close() {
        this.start.compareAndSet(true, false);
        this.worker.interrupt();
        this.pushTcpServer.close();
    }


}
