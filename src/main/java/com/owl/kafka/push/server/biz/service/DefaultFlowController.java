package com.owl.kafka.push.server.biz.service;

import com.owl.kafka.proxy.transport.protocol.Packet;
import com.owl.kafka.push.server.biz.push.PushCenter;
import com.owl.kafka.push.server.biz.bo.ControlResult;
import com.owl.kafka.push.server.biz.bo.ServerConfigs;
import com.owl.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class DefaultFlowController implements FlowController<Packet> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushCenter.class);

    private final long ALLOW_MOMERY_SIZE = ServerConfigs.I.getServerFlowControlMessageSize() * Constants.M_BYTES;

    private final long ALLOW_COUNT = ServerConfigs.I.getServerFlowControlMessageCount();

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
