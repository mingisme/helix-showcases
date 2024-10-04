package com.envisioniot.enos;

import java.util.concurrent.atomic.AtomicInteger;

public class MessageCounter {
    public static AtomicInteger produceCount = new AtomicInteger(0);
    public static AtomicInteger consumeCount = new AtomicInteger(0);;
}
