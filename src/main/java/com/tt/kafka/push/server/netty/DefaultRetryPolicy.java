package com.tt.kafka.push.server.netty;

/**
 * @Author: Tboy
 */
public class DefaultRetryPolicy implements RetryPolicy {

    private final int retryCountReplica;

    private int retryCount;

    private long retryPeriod;

    public DefaultRetryPolicy(int retryCount, long retryPeriod){
        this.retryCount = retryCount;
        this.retryCountReplica = retryCount;
        this.retryPeriod = retryPeriod;
    }

    @Override
    public boolean allowRetry() throws InterruptedException{
        if(retryCount > 0){
            Thread.sleep(retryPeriod);
            retryPeriod--;
            return true;
        }
        return false;
    }

    @Override
    public void reset() {
        retryCount = retryCountReplica;
    }
}
