package com.tt.kafka.push.server;

/**
 * @Author: Tboy
 */
public interface Constants {

    int CPU_SIZE = Runtime.getRuntime().availableProcessors();

    String PUSH_SERVER_PORT = "push.server.port";

    String PUSH_SERVER_BOSS_NUM = "push.server.boss.num";

    String PUSH_SERVER_WORKER_NUM = "push.server.worker.num";
}
