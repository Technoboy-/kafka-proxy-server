package com.owl.kafka.proxy.server.biz.service;


import com.owl.kafka.client.util.NamedThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SystemClock is a optimized substitute of {@link System#currentTimeMillis()} for avoiding context switch overload
 * @Author: Tboy
 */
public class SystemClock {

    private static final SystemClock MILLI_CLOCK = new SystemClock(1);

    private final long precision;
    private final AtomicLong now;

    public static SystemClock millisClock() {
        return MILLI_CLOCK;
    }

    private SystemClock(long precision) {
        this.precision = precision;
        now = new AtomicLong(System.currentTimeMillis());
        scheduleClockUpdating();
    }

    private void scheduleClockUpdating() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("system.clock"));

        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                now.set(System.currentTimeMillis());
            }
        }, precision, precision, TimeUnit.MILLISECONDS);
    }

    public long now() {
        return now.get();
    }

    public long precision() {
        return precision;
    }
}
