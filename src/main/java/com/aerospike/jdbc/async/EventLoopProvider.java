package com.aerospike.jdbc.async;

import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import io.netty.channel.nio.NioEventLoopGroup;

import static java.util.Objects.requireNonNull;

public final class EventLoopProvider {

    private static EventLoops eventLoops;

    private EventLoopProvider() {
    }

    public static EventLoop getEventLoop() {
        initEventLoops();
        return eventLoops.next();
    }

    public static EventLoops getEventLoops() {
        initEventLoops();
        return eventLoops;
    }

    private static void initEventLoops() {
        if (null == eventLoops) {
            int nThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
            eventLoops = new NettyEventLoops(new EventPolicy(), new NioEventLoopGroup(nThreads));
            requireNonNull(eventLoops.get(0));
        }
    }
}
