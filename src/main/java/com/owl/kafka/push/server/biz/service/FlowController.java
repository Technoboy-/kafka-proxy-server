package com.owl.kafka.push.server.biz.service;

import com.owl.kafka.push.server.biz.bo.ControlResult;

/**
 * @Author: Tboy
 */
public interface FlowController<T> {

    ControlResult flowControl(T t);
}
