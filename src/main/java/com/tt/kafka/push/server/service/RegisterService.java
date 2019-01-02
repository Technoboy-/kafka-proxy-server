package com.tt.kafka.push.server.service;

import com.tt.kafka.push.server.zookeeper.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class RegisterService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterService.class);

    private static final String PROVIDER_PATH = "/%s/providers";

    private final ZookeeperClient client;

    public RegisterService(ZookeeperClient client){
        this.client = client;
    }

    public void register(RegisterMetadata metadata){
        try {
            if(!client.checkExists(String.format(PROVIDER_PATH, metadata.getTopic()))){
                client.createPersistent(String.format(PROVIDER_PATH, metadata.getTopic()));
            }
            client.createEPhemeral(String.format(PROVIDER_PATH, metadata.getTopic()) +
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

    public static void main(String[] args) {
        System.out.println(String.format(PROVIDER_PATH, "test-topic") +
                String.format("%s:%s", "localhost", 10666));
    }
}
