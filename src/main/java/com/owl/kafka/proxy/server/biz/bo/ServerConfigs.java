package com.owl.kafka.proxy.server.biz.bo;


import com.owl.kafka.client.proxy.ConfigLoader;
import com.owl.kafka.client.util.Constants;
import com.owl.kafka.client.util.Preconditions;
import com.owl.kafka.client.util.StringUtils;

/**
 * @Author: Tboy
 */
public class ServerConfigs extends ConfigLoader {

    static final String SERVER_PORT = "server.port";

    static final String SERVER_BOSS_NUM = "server.boss.num";

    static final String SERVER_WORKER_NUM = "server.worker.num";

    static final String SERVER_QUEUE_SIZE = "server.queue.size";

    static final String SERVER_TOPIC = "server.topic";

    static final String SERVER_GROUP_ID = "server.group.id";

    static final String SERVER_KAFKA_SERVER_LIST = "server.kafka.server.list";

    static final String SERVER_COMMIT_OFFSET_INTERVAL = "server.commit.offset.interval";

    static final String SERVER_COMMIT_OFFSET_BATCH_SIZE = "server.commit.offset.batch.size";

    static final String SERVER_MESSAGE_REPOST_TIMES = "server.message.repost.times";

    static final String SERVER_MESSAGE_REPOST_INTERVAL = "server.message.repost.interval";

    static final String SERVER_PUSH_FLOW_CONTROL_MESSAGE_COUNT = "server.push.flow.control.message.count";

    static final String SERVER_PUSH_FLOW_CONTROL_MESSAGE_SIZE = "server.push.flow.control.message.size";

    static final String SERVER_PULL_MESSAGE_COUNT = "server.pull.message.count";

    static final String SERVER_PULL_MESSAGE_SIZE = "server.pull.message.size";

    static final String SERVER_REPOST_COUNT = "server.repost.count";

    static final  String SERVER_CONFIG_FILE = "proxy_server.properties";

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

    public int getServerCommitOffsetInterval() {
        return getInt(SERVER_COMMIT_OFFSET_INTERVAL, 30);
    }

    public int getServerCommitOffsetBatchSize() {
        return getInt(SERVER_COMMIT_OFFSET_BATCH_SIZE, 10000);
    }

    public int getServerMessageRepostTimes() {
        return getInt(SERVER_MESSAGE_REPOST_TIMES, 10);
    }

    public int getServerMessageRepostInterval() {
        return getInt(SERVER_MESSAGE_REPOST_INTERVAL, 3);
    }

    public int getServerFlowControlMessageCount() {
        return getInt(SERVER_PUSH_FLOW_CONTROL_MESSAGE_COUNT, 10000);
    }

    public int getServerFlowControlMessageSize() {
        return getInt(SERVER_PUSH_FLOW_CONTROL_MESSAGE_SIZE, 64);
    }

    public int getServerPullMessageCount(){
        return getInt(SERVER_PULL_MESSAGE_COUNT, 10);
    }

    public long getServerPullMessageSize(){
        return getLong(SERVER_PULL_MESSAGE_SIZE, 1024 * 1024 * 8);
    }

    public int getServerRepostCount(){
        return getInt(SERVER_REPOST_COUNT, 5);
    }
}
