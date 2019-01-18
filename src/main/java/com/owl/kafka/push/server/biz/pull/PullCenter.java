package com.owl.kafka.push.server.biz.pull;

import com.owl.kafka.client.service.IdService;
import com.owl.kafka.client.transport.protocol.Command;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.serializer.SerializerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Author: Tboy
 */
public class PullCenter{

    private static final Logger LOGGER = LoggerFactory.getLogger(PullCenter.class);

    public static PullCenter I  = new PullCenter();

    private final int queueSize = ServerConfigs.I.getServerQueueSize();

    private final LinkedBlockingQueue<Packet> retryQueue = new LinkedBlockingQueue<>(queueSize);

    private final LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>> pullQueue = new LinkedBlockingQueue<>(queueSize);

    public LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>> getPullQueue() {
        return pullQueue;
    }

    public LinkedBlockingQueue<Packet> getRetryQueue() {
        return retryQueue;
    }

    public List<Packet> pull(long messageCount, long messageSize) {
        List<Packet> results = new ArrayList<>();
        while(messageCount < results.size() && calculateSize(results) < messageSize){
            Packet poll = poll();
            if(poll == null){
                break;
            } else{
                results.add(poll);
            }
        }
        return results;
    }

    private Packet poll() {
        Packet packet = retryQueue.peek();
        if(packet != null){
            retryQueue.poll();
        } else{
            ConsumerRecord<byte[], byte[]> record = pullQueue.poll();
            if(record != null){
                packet = new Packet();
                //
                packet.setCmd(Command.PUSH.getCmd());
                packet.setMsgId(IdService.I.getId());
                Header header = new Header(record.topic(), record.partition(), record.offset());
                header.setRepost(1);
                packet.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
                packet.setKey(record.key());
                packet.setValue(record.value());
            }
            //
        }
        return packet;
    }

    private long calculateSize(List<Packet> records){
        long size = 0;
        for(Packet record : records){
            size = size + 1 + 1 + 8 + record.getHeader().length + record.getKey().length + record.getValue().length;
        }
        return size;
    }
}
