package com.tt.kafka.push.server.service;

import com.tt.kafka.push.server.zookeeper.ZookeeperClient;
import com.tt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class RegisterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterService.class);

    private final ZookeeperClient client;

    public RegisterService(ZookeeperClient client){
        this.client = client;
    }

    public void register(RegisterMetadata metadata){
        try {
            String topic = String.format(Constants.ZOOKEEPER_PROVIDERS, metadata.getTopic());
            if(!client.checkExists(topic)){
                client.createPersistent(topic);
            }
            client.createEPhemeral(topic +
                    String.format("/%s:%s", metadata.getAddress().getHost(), metadata.getAddress().getPort()), new ZookeeperClient.BackgroundCallback() {
                @Override
                public void complete() {
                    //TODO
                    //for monitor use
                }
            });
        } catch (Exception ex) {
            LOGGER.error("register service error", ex);
            throw new RuntimeException(ex);
        }
    }
}
