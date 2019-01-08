package com.tt.kafka.push.server.biz.service;

import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.push.server.biz.PushCenter;
import com.tt.kafka.push.server.biz.bo.ControlResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class DefaultFlowController implements FlowController<Packet> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushCenter.class);

    private static final long ALLOW_MOMERY_SIZE = 64 * 1024 * 1024;

    private static final long ALLOW_COUNT = 10000;

    @Override
    public ControlResult flowControl(Packet packet) {
        ControlResult result = ControlResult.ALLOWED;

        if(MessageHolder.memorySize() > ALLOW_MOMERY_SIZE){
            result = new ControlResult(false,
                    "message size overflow, real size : " + MessageHolder.memorySize() + " threshold : " + ALLOW_MOMERY_SIZE);
            doFlowControl();
            return result;
        }
        if(MessageHolder.count() > ALLOW_COUNT){
            result = new ControlResult(false,
                    "message count overflow, real count : " + MessageHolder.count() + " threshold : " + ALLOW_COUNT);
            doFlowControl();
            return result;
        }
        return result;
    }

    private void doFlowControl(){
        LOGGER.warn("do memory flow control ...");
        try {
            TimeUnit.MILLISECONDS.sleep(5);
        } catch (InterruptedException e) {
            //
        }
    }
}
