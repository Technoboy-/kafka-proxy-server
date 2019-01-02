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
        String port = System.getProperty(Constants.PUSH_SERVER_BOSS_NUM, properties.getProperty(Constants.PUSH_SERVER_BOSS_NUM, "10666"));
        return Integer.valueOf(port);
    }

    public int getWorkerNum(){
        String port = System.getProperty(Constants.PUSH_SERVER_WORKER_NUM, properties.getProperty(Constants.PUSH_SERVER_WORKER_NUM, String.valueOf(Constants.CPU_SIZE)));
        return Integer.valueOf(port);
    }

    public int getPort() {
        String port = System.getProperty(Constants.PUSH_SERVER_PORT, properties.getProperty(Constants.PUSH_SERVER_PORT, "10666"));
        return Integer.valueOf(port);
    }
}
