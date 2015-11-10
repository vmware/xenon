/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.dcp.common.http.netty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLContext;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceHost.ServiceHostState;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;

/**
 * Asynchronous connection management pool
 */
public class NettyChannelPool {

    public static class NettyChannelGroup {
        public List<NettyChannelContext> availableChannels = new ArrayList<>();
        public Set<NettyChannelContext> inUseChannels = new HashSet<>();
        public List<Operation> pendingRequests = new ArrayList<>();
    }

    private static final long CHANNEL_EXPIRATION_MICROS =
            ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS * 2;

    public static String toConnectionKey(String host, int port) {
        return host + port;
    }

    private final ExecutorService executor;
    private EventLoopGroup eventGroup;
    private String threadTag = NettyChannelPool.class.getSimpleName();
    private int threadCount;

    private Bootstrap bootStrap;

    private final Map<String, NettyChannelGroup> channelGroups = new ConcurrentSkipListMap<>();
    private int connectionLimit = 1;

    private SSLContext sslContext;


    public NettyChannelPool(ExecutorService executor) {
        this.executor = executor;
    }

    public NettyChannelPool setThreadTag(String tag) {
        this.threadTag = tag;
        return this;
    }

    public NettyChannelPool setThreadCount(int count) {
        this.threadCount = count;
        return this;
    }

    public void start() {
        if (this.bootStrap != null) {
            return;
        }

        this.eventGroup = new NioEventLoopGroup(this.threadCount, (t) -> {
            return Executors.newFixedThreadPool(t,
                    r -> new Thread(r, this.threadTag));
        });
        this.bootStrap = new Bootstrap();
        this.bootStrap.group(this.eventGroup)
                .channel(NioSocketChannel.class)
                .handler(new NettyHttpClientRequestInitializer(this));
    }

    public boolean isStarted() {
        return this.bootStrap != null;
    }

    public NettyChannelPool setConnectionLimitPerHost(int limit) {
        this.connectionLimit = limit;
        return this;
    }

    public int getConnectionLimitPerHost() {
        return this.connectionLimit;
    }

    private NettyChannelGroup getChannelGroup(String key) {
        NettyChannelGroup group;
        synchronized (this.channelGroups) {
            group = this.channelGroups.get(key);
            if (group == null) {
                group = new NettyChannelGroup();
                this.channelGroups.put(key, group);
            }
        }
        return group;
    }

    public long getPendingRequestCount(Operation op) {
        String key = toConnectionKey(op.getUri().getHost(), op.getUri().getPort());
        NettyChannelGroup group = getChannelGroup(key);
        return group.pendingRequests.size();
    }

    public void connectOrReuse(String host, int port, boolean doNotReUse,
            Operation request) {

        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }

        if (host == null) {
            request.fail(new IllegalArgumentException("host is required"));
            return;
        }

        if (port <= 0) {
            port = UriUtils.HTTP_DEFAULT_PORT;
        }

        try {
            String key = toConnectionKey(host, port);
            NettyChannelGroup group = getChannelGroup(key);
            NettyChannelContext context = null;

            synchronized (group) {
                if (!group.availableChannels.isEmpty() && !doNotReUse) {
                    context = group.availableChannels.remove(group.availableChannels.size() - 1);
                    context.updateLastUseTime();
                } else if (group.inUseChannels.size() >= this.connectionLimit) {
                    group.pendingRequests.add(request);
                    return;
                } else {
                    context = new NettyChannelContext(host, port, key);
                }

                if (context.getChannel() != null) {
                    if (!context.getChannel().isOpen()) {
                        context.close();
                        context = new NettyChannelContext(host, port, key);
                    }
                }

                group.inUseChannels.add(context);
            }

            NettyChannelContext contextFinal = context;

            if (context.getChannel() != null) {
                context.setOperation(request);
                request.complete();
                return;
            }

            ChannelFuture connectFuture = this.bootStrap.connect(context.host, context.port);
            connectFuture.addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception {

                    if (future.isSuccess()) {
                        Channel ch = future.channel();
                        contextFinal.setChannel(ch).setOperation(request);
                        request.complete();
                    } else {
                        returnOrClose(contextFinal, true);
                        fail(request, future.cause());
                    }
                }

            });

        } catch (Throwable e) {
            fail(request, e);
        }
    }

    private void fail(Operation request, Throwable e) {
        request.fail(e, Operation.STATUS_CODE_BAD_REQUEST);
    }

    public void returnOrClose(NettyChannelContext context, boolean isClose) {
        ExecutorService e = this.executor;
        if (e == null) {
            return;
        }
        if (e.isShutdown()) {
            return;
        }
        if (context == null) {
            return;
        }

        // execute in new thread, to avoid large call stacks when we process a
        // lot of pending requests, due to failure
        e.execute(() -> returnOrCloseDirect(context, isClose));
    }

    boolean isContextInUse(NettyChannelContext context) {
        if (context == null) {
            return false;
        }
        NettyChannelGroup group = this.channelGroups.get(context.getKey());
        return group != null && group.inUseChannels.contains(context);
    }

    private void returnOrCloseDirect(NettyChannelContext context, boolean isClose) {
        Operation pendingOp = null;
        Channel ch = context.getChannel();
        isClose = isClose || !ch.isWritable() || !ch.isOpen();
        NettyChannelGroup group = this.channelGroups.get(context.getKey());
        if (group == null) {
            context.close();
            return;
        }
        synchronized (group) {
            if (!group.pendingRequests.isEmpty()) {
                pendingOp = group.pendingRequests
                        .remove(group.pendingRequests.size() - 1);
            }

            if (isClose) {
                group.inUseChannels.remove(context);
            } else {
                if (pendingOp == null) {
                    group.availableChannels.add(context);
                    group.inUseChannels.remove(context);
                }
            }
        }

        if (isClose) {
            context.close();
        }

        if (pendingOp == null) {
            return;
        }

        if (isClose) {
            connectOrReuse(context.host, context.port, false, pendingOp);
        } else {
            context.setOperation(pendingOp);
            pendingOp.complete();
        }
    }

    public void stop() {
        try {
            for (NettyChannelGroup g : this.channelGroups.values()) {
                synchronized (g) {
                    for (NettyChannelContext c : g.availableChannels) {
                        c.close();
                    }
                    for (NettyChannelContext c : g.inUseChannels) {
                        c.close();
                    }
                    g.availableChannels.clear();
                    g.inUseChannels.clear();
                }
            }
            this.eventGroup.shutdownGracefully();
        } catch (Throwable e) {
            // ignore exception
        }
        this.bootStrap = null;
    }

    public void handleMaintenance(Operation op) {
        for (NettyChannelGroup g : this.channelGroups.values()) {
            synchronized (g) {
                closeContexts(g.availableChannels, false);
                closeExpiredInUseContext(g.inUseChannels);
            }
        }
        op.complete();
    }

    private void closeExpiredInUseContext(Collection<NettyChannelContext> contexts) {
        long now = Utils.getNowMicrosUtc();
        for (NettyChannelContext c : contexts) {
            Operation activeOp = c.getOperation();
            if (activeOp == null || activeOp.getExpirationMicrosUtc() > now) {
                continue;
            }
            this.executor.execute(() -> {
                // client has nested completion on failure, and will close context
                activeOp.fail(new TimeoutException(activeOp.toString()));
            });
            continue;
        }
    }

    private void closeContexts(Collection<NettyChannelContext> contexts, boolean forceClose) {
        long now = Utils.getNowMicrosUtc();
        List<NettyChannelContext> items = new ArrayList<>();
        for (NettyChannelContext c : contexts) {
            try {
                if (c.getChannel() == null || !c.getChannel().isOpen()) {
                    continue;
                }
            } catch (Throwable e) {
            }

            long delta = now - c.getLastUseTimeMicros();
            if (!forceClose && delta < CHANNEL_EXPIRATION_MICROS) {
                continue;
            }
            c.close();
            items.add(c);
        }
        for (NettyChannelContext c : items) {
            contexts.remove(c);
        }
    }

    public void setSSLContext(SSLContext context) {
        if (isStarted()) {
            throw new IllegalStateException("Already started");
        }
        this.sslContext = context;
    }

    public SSLContext getSSLContext() {
        return this.sslContext;
    }
}
