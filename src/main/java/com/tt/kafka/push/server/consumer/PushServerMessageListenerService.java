package com.tt.kafka.push.server.consumer;

import com.tt.kafka.consumer.service.RebalanceMessageListenerService;
import com.tt.kafka.netty.protocol.Command;
import com.tt.kafka.netty.protocol.Header;
import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.service.IdService;
import com.tt.kafka.push.server.PushServerConfigs;
import com.tt.kafka.push.server.netty.PushTcpServer;
import com.tt.kafka.serializer.SerializerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PushServerMessageListenerService<K, V> extends RebalanceMessageListenerService<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServerMessageListenerService.class);

    private final PushServerConfigs configs;

    private final PushTcpServer pushTcpServer;

    private final IdService idService;

    public PushServerMessageListenerService(){
        this.configs = new PushServerConfigs();
        this.idService = new IdService();
        this.pushTcpServer = new PushTcpServer(configs);
        this.pushTcpServer.start();
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        Packet packet = new Packet();
        packet.setCmd(Command.PUSH.getCmd());
        packet.setMsgId(idService.getId());
        Header header = new Header(record.topic(), record.partition(), record.offset());
        packet.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
        packet.setKey(record.key());
        packet.setValue(record.value());
        //
        pushTcpServer.push(packet);
    }

    @Override
    public void close() {
        pushTcpServer.close();
    }
}
