package com.tt.kafka.push.server.consumer;

import com.tt.kafka.consumer.service.RebalanceMessageListenerService;
import com.tt.kafka.client.netty.protocol.Command;
import com.tt.kafka.client.netty.protocol.Header;
import com.tt.kafka.client.netty.protocol.Packet;
import com.tt.kafka.client.netty.service.IdService;
import com.tt.kafka.push.server.PushServerConfigs;
import com.tt.kafka.push.server.netty.PushTcpServer;
import com.tt.kafka.serializer.SerializerImpl;
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

    private final LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>> queue;

    private final LinkedBlockingQueue<Packet> retryQueue;

    private final IdService idService;

    private final PushTcpServer pushTcpServer;

    private final Thread worker;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public PushServerMessageListenerService(PushTcpServer pushTcpServer, PushServerConfigs serverConfigs){
        this.queue = new LinkedBlockingQueue<>(serverConfigs.getPushServerQueueSize());
        this.retryQueue = new LinkedBlockingQueue<>(serverConfigs.getPushServerQueueSize());
        this.idService = new IdService();
        this.pushTcpServer = pushTcpServer;
        this.start.compareAndSet(false, true);
        this.worker = new Thread(this,"push-worker");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        try {
            queue.put(record);
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
                    ConsumerRecord<byte[], byte[]> record = queue.take();
                    packet = new Packet();
                    //
                    packet.setCmd(Command.PUSH.getCmd());
                    packet.setMsgId(idService.getId());
                    Header header = new Header(record.topic(), record.partition(), record.offset());
                    packet.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
                    packet.setKey(record.key());
                    packet.setValue(record.value());
                    //
                }
                pushTcpServer.push(packet, retryQueue);
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
