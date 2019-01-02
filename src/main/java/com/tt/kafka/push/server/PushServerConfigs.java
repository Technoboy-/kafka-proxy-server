package com.tt.kafka.push.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;


/**
 * @Author: Tboy
 */
public class PushServerConfigs {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServerConfigs.class);

    private static final String CONFIG_FILE = "push_server.properties";

    private final Properties properties;

    public PushServerConfigs(){
        this.properties = new Properties();
        load();
    }

    private void load(){
        InputStream fis = null;
        try {
            URL resource = PushServerConfigs.class.getClassLoader().getResource(CONFIG_FILE);
            if(resource == null){
                resource = PushServerConfigs.class.getResource(CONFIG_FILE);
            }
            if(resource != null){
                fis = resource.openStream();
            }
            if(fis == null){
                fis = new FileInputStream(new File(CONFIG_FILE));
            }
        } catch (Exception ex) {
            LOGGER.error("error", ex);
        }
        if(fis == null){
            throw new RuntimeException("push_server.properties not found");
        }
        try {
            properties.load(fis);
        } catch (IOException ex) {
            LOGGER.error("error", ex);
            throw new RuntimeException(ex);
        }
    }

    public int getBossNum(){
        String bossNum = System.getProperty(Constants.PUSH_SERVER_BOSS_NUM, properties.getProperty(Constants.PUSH_SERVER_BOSS_NUM));
        return Integer.getInteger(bossNum, 1);
    }

    public int getWorkerNum(){
        String workerNum = System.getProperty(Constants.PUSH_SERVER_WORKER_NUM, properties.getProperty(Constants.PUSH_SERVER_WORKER_NUM));
        return Integer.getInteger(workerNum, Constants.CPU_SIZE);
    }

    public int getPort() {
        String port = System.getProperty(Constants.PUSH_SERVER_PORT, properties.getProperty(Constants.PUSH_SERVER_PORT));
        return Integer.getInteger(port, 10666);
    }

    public String getGroupId() {
        return System.getProperty(Constants.PUSH_SERVER_GROUP_ID, properties.getProperty(Constants.PUSH_SERVER_GROUP_ID));
    }

    public String getKafkaServerList() {
        return System.getProperty(Constants.PUSH_SERVER_KAFKA_SERVER_LIST, properties.getProperty(Constants.PUSH_SERVER_KAFKA_SERVER_LIST));
    }

    public String getZookeeperServerList(){
        return System.getProperty(Constants.ZOOKEEPER_SERVER_LIST, properties.getProperty(Constants.ZOOKEEPER_SERVER_LIST));
    }

    public String getZookeeperNamespace(){
        return System.getProperty(Constants.ZOOKEEPER_NAMESPACE, properties.getProperty(Constants.ZOOKEEPER_NAMESPACE, "push_server"));
    }

    public int getZookeeperSessionTimeoutMs(){
        String sessionTimeoutMs = System.getProperty(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS, properties.getProperty(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS));
        return Integer.getInteger(sessionTimeoutMs, 60 * 1000);
    }

    public int getZookeeperConnectionTimeoutMs(){
        String connectionTimeoutMs =  System.getProperty(Constants.ZOOKEEPER_CONNECTION_TIMEOUT_MS, properties.getProperty(Constants.ZOOKEEPER_CONNECTION_TIMEOUT_MS));
        return Integer.getInteger(connectionTimeoutMs, 15 * 1000);
    }

    public String getZookeeperTopic(){
        return System.getProperty(Constants.ZOOKEEPER_TOPIC, properties.getProperty(Constants.ZOOKEEPER_TOPIC));
    }

    public int getPushServerQueueSize(){
        String queueSize = System.getProperty(Constants.PUSH_SERVER_QUEUE_SIZE, properties.getProperty(Constants.PUSH_SERVER_QUEUE_SIZE));
        return Integer.getInteger(queueSize, 100);
    }


}
