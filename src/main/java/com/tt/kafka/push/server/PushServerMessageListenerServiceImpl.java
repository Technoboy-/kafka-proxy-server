package com.tt.kafka.push.server;

import com.tt.kafka.consumer.Record;
import com.tt.kafka.consumer.service.RebalanceMessageListenerService;
import com.tt.kafka.netty.protocol.Command;
import com.tt.kafka.netty.protocol.Header;
import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.service.IdService;
import com.tt.kafka.netty.transport.Connection;
import com.tt.kafka.push.server.netty.InvokePolicy;
import com.tt.kafka.push.server.netty.LoadBalanceInvokePolicy;
import com.tt.kafka.serializer.SerializerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PushServerMessageListenerServiceImpl<K, V> extends RebalanceMessageListenerService<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServerMessageListenerServiceImpl.class);

    private static final int PORT = 10666;

    private final TTKafkaPushServer pushServer;

    private final InvokePolicy invokePolicy;

    private final IdService idService;

    public PushServerMessageListenerServiceImpl(){
        this.idService = new IdService();
        this.pushServer = new TTKafkaPushServer(PORT);
        this.invokePolicy = new LoadBalanceInvokePolicy(this.pushServer.getClientRegistry());
        this.pushServer.start();
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
        Connection connection = invokePolicy.getConnection();
        if(connection != null){
            connection.send(packet);
        }
    }

    public Record toRecord(ConsumerRecord<byte[], byte[]> record) {
        return new Record(record.topic(), record.partition(), record.offset(),
                record.key(),
                record.value(),
                record.timestamp());
    }

    @Override
    public void close() {
        pushServer.close();
    }
}
