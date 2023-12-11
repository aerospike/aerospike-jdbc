package com.aerospike.jdbc.async;

import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

public final class EventLoopProvider {

    private static final Logger logger = Logger.getLogger(EventLoopProvider.class.getName());
    private static volatile EventLoops eventLoops;

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

    public static synchronized void close() {
        if (null != eventLoops) {
            logger.info(() -> "Close eventLoops");
            eventLoops.close();
            eventLoops = null;
        }
    }

    private static void initEventLoops() {
        if (null == eventLoops) {
            synchronized (EventLoopProvider.class) {
                if (null == eventLoops) {
                    logger.info(() -> "Init eventLoops");
                    int nThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
                    EventLoops nettyEventLoops = new NettyEventLoops(
                            new EventPolicy(),
                            new NioEventLoopGroup(nThreads)
                    );
                    requireNonNull(nettyEventLoops.get(0));
                    eventLoops = nettyEventLoops;
                }
            }
        }
    }
}
