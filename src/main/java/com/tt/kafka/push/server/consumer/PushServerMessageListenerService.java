package com.tt.kafka.push.server.consumer;

import com.tt.kafka.consumer.service.RebalanceMessageListenerService;
import com.tt.kafka.netty.protocol.Command;
import com.tt.kafka.netty.protocol.Header;
import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.service.IdService;
import com.tt.kafka.netty.transport.Connection;
import com.tt.kafka.push.server.PushServerConfigs;
import com.tt.kafka.push.server.TTKafkaPushServer;
import com.tt.kafka.push.server.netty.LoadBalancePolicy;
import com.tt.kafka.push.server.netty.RoundRobinPolicy;
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

    private final TTKafkaPushServer pushServer;

    private final LoadBalancePolicy loadBalancePolicy;

    private final IdService idService;

    public PushServerMessageListenerService(){
        this.configs = new PushServerConfigs();
        this.idService = new IdService();
        this.pushServer = new TTKafkaPushServer(configs);
        this.loadBalancePolicy = new RoundRobinPolicy(this.pushServer.getClientRegistry());
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
        Connection connection = loadBalancePolicy.getConnection();
        if(connection != null){
            connection.send(packet);
        }
    }

    @Override
    public void close() {
        pushServer.close();
    }
}
