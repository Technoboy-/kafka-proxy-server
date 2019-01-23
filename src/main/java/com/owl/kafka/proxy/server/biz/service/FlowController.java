package com.owl.kafka.proxy.server.biz.service;

import com.owl.kafka.proxy.server.biz.bo.ControlResult;

/**
 * @Author: Tboy
 */
public interface FlowController<T> {

    ControlResult flowControl(T t);
}
