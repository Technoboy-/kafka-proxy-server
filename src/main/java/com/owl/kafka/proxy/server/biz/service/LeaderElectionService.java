package com.owl.kafka.proxy.server.biz.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author: Tboy
 */
public class LeaderElectionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectionService.class);

    private final LeaderLatch leaderLatch;

    public LeaderElectionService(CuratorFramework client, String path, LeaderLatchListener listener){
        this.leaderLatch = new LeaderLatch(client, path);
        this.leaderLatch.addListener(listener);
        try {
            this.leaderLatch.start();
        } catch (Exception ex) {
            LOGGER.error("start leaderLatch error", ex);
        }
    }

    public boolean hasLeadership(){
        return this.leaderLatch.hasLeadership();
    }

    public void select() throws Exception{
        this.leaderLatch.await();
    }

    public void close(){
        try {
            this.leaderLatch.close();
        } catch (IOException ex) {
            LOGGER.error("close leaderLatch error", ex);
        }
    }
}
