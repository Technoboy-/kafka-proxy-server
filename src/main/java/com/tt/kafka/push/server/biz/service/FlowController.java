package com.tt.kafka.push.server.biz.service;

import com.tt.kafka.push.server.biz.bo.ControlResult;

/**
 * @Author: Tboy
 */
public interface FlowController<T> {

    ControlResult flowControl(T t);
}
