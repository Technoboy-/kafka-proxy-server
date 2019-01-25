package com.owl.kafka.proxy.server.biz.pull;

/**
 * @Author: Tboy
 */
public class Test {

    public static void main(String[] args) {
        int normCapacity = 511;
        System.out.println((normCapacity & 0xFFFFFE00) == 0);
        System.out.println(0xFFFFFE00);
        System.out.println(~4);
        System.out.println(4 & ~4);
        System.out.println(512 >>> 4);
    }
}
