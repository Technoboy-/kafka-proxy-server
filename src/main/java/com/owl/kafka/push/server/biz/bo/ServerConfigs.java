package com.owl.kafka.push.server.biz.bo;

import com.owl.kafka.util.Constants;
import com.owl.kafka.util.Preconditions;
import com.owl.kafka.util.StringUtils;
import com.owl.kafka.client.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class ServerConfigs extends ConfigLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConfigs.class);

    // server configs
    static final String SERVER_PORT = "server.port";

    static final String SERVER_BOSS_NUM = "server.boss.num";

    static final String SERVER_WORKER_NUM = "server.worker.num";

    static final String SERVER_QUEUE_SIZE = "server.queue.size";

    static final String SERVER_TOPIC = "server.topic";

    static final String SERVER_GROUP_ID = "server.group.id";

    static final String SERVER_KAFKA_SERVER_LIST = "server.kafka.server.list";

    static final  String SERVER_CONFIG_FILE = "push_server.properties";

    public static ServerConfigs I = new ServerConfigs(SERVER_CONFIG_FILE);

    public ServerConfigs(String fileName){
        super(fileName);
    }

    protected void afterLoad(){
        Preconditions.checkArgument(!StringUtils.isBlank(getServerTopic()), "topic should not be empty");
        //
        Preconditions.checkArgument(!StringUtils.isBlank(getServerGroupId()), "groupId should not be empty");
    }

    public int getServerPort(){
        return getInt(SERVER_PORT, 10666);
    }

    public int getServerBossNum(){
        return getInt(SERVER_BOSS_NUM, 1);
    }

    public int getServerWorkerNum(){
        return getInt(SERVER_WORKER_NUM, Constants.CPU_SIZE + 1);
    }

    public String getServerGroupId() {
        return get(SERVER_GROUP_ID);
    }

    public String getServerTopic() {
        return get(SERVER_TOPIC);
    }

    public int getServerQueueSize() {
        return getInt(SERVER_QUEUE_SIZE, 100);
    }

    public String getServerKafkaServerList() {
        return get(SERVER_KAFKA_SERVER_LIST);
    }

}
