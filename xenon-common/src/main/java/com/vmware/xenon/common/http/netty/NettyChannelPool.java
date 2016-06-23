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

package com.vmware.xenon.common.http.netty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Asynchronous connection management pool
 */
public class NettyChannelPool {

    public static class NettyChannelGroupKey implements Comparable<NettyChannelGroupKey> {
        private final String connectionTag;
        private final String host;
        private final int port;
        private int hashcode;

        public NettyChannelGroupKey(String tag, String host, int port, boolean isHttp2) {
            if (tag == null) {
                tag = isHttp2 ? ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT
                        : ServiceClient.CONNECTION_TAG_DEFAULT;
            }
            this.connectionTag = tag;
            // a null host is not valid but we do URI validation elsewhere. If we are called with null host
            // here it means we are trying to fail the operation that failed the validation, so use empty string
            this.host = host == null ? "" : host;
            if (port <= 0) {
                port = UriUtils.HTTP_DEFAULT_PORT;
            }
            this.port = port;
        }

        @Override
        public String toString() {
            return this.connectionTag + ":" + this.host + ":" + this.port;
        }

        @Override
        public int hashCode() {
            if (this.hashcode == 0) {
                this.hashcode = Objects.hash(this.connectionTag, this.host, this.port);
            }
            return this.hashcode;
        }

        @Override
        public int compareTo(NettyChannelGroupKey o) {
            int r = Integer.compare(this.port, o.port);
            if (r != 0) {
                return r;
            }
            r = this.connectionTag.compareTo(o.connectionTag);
            if (r != 0) {
                return r;
            }
            return this.host.compareTo(o.host);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof NettyChannelGroupKey)) {
                return false;
            }
            NettyChannelGroupKey otherKey = (NettyChannelGroupKey) other;
            return compareTo(otherKey) == 0;
        }
    }

    public static class NettyChannelGroup {
        private NettyChannelGroupKey key;

        public NettyChannelGroup(NettyChannelGroupKey key) {
            this.key = key;
        }

        public NettyChannelGroupKey getKey() {
            return this.key;
        }

        // Available channels are for when we have an HTTP/1.1 connection
        public Queue<NettyChannelContext> availableChannels = new ConcurrentLinkedQueue<>();

        public List<NettyChannelContext> inUseChannels = new ArrayList<>();
        public Queue<Operation> pendingRequests = new ConcurrentLinkedQueue<>();
    }

    public static final Logger LOGGER = Logger.getLogger(NettyChannelPool.class
            .getName());

    private static final long CHANNEL_EXPIRATION_MICROS = Long.getLong(
            Utils.PROPERTY_NAME_PREFIX + "NettyChannelPool.CHANNEL_EXPIRATION_MICROS",
            ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS * 10);

    private ExecutorService nettyExecutorService;
    private ExecutorService executor;
    private EventLoopGroup eventGroup;
    private String threadTag = NettyChannelPool.class.getSimpleName();
    private int threadCount;
    private boolean isHttp2Only = false;
    private Bootstrap bootStrap;

    private final Map<NettyChannelGroupKey, NettyChannelGroup> channelGroups = new ConcurrentSkipListMap<>();
    private Map<String, Integer> connectionLimitsPerTag = new ConcurrentSkipListMap<>();

    private int connectionLimit = 1;

    private SSLContext sslContext;

    private int requestPayloadSizeLimit;

    public NettyChannelPool() {
    }

    public NettyChannelPool setThreadTag(String tag) {
        this.threadTag = tag;
        return this;
    }

    public NettyChannelPool setThreadCount(int count) {
        this.threadCount = count;
        return this;
    }

    public NettyChannelPool setExecutor(ExecutorService es) {
        this.executor = es;
        return this;
    }

    /**
     * Force the channel pool to be HTTP/2.
     */
    public NettyChannelPool setHttp2Only() {
        this.isHttp2Only = true;
        return this;
    }

    /**
     * Returns true if the channel pool is for HTTP/2
     */
    public boolean isHttp2Only() {
        return this.isHttp2Only;
    }

    public void start() {
        if (this.bootStrap != null) {
            return;
        }

        if (this.executor == null) {
            this.nettyExecutorService = Executors.newFixedThreadPool(this.threadCount,
                    r -> new Thread(
                            r, this.threadTag));
            this.executor = this.nettyExecutorService;
        }
        this.eventGroup = new NioEventLoopGroup(this.threadCount, this.executor);

        this.bootStrap = new Bootstrap();
        this.bootStrap.group(this.eventGroup)
                .channel(NioSocketChannel.class)
                .handler(new NettyHttpClientRequestInitializer(this, this.isHttp2Only,
                        this.requestPayloadSizeLimit));
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

    public void setConnectionLimitPerTag(String tag, int limit) {
        this.connectionLimitsPerTag.put(tag, limit);
    }

    public int getConnectionLimitPerTag(String tag) {
        return this.connectionLimitsPerTag.getOrDefault(tag,
                ServiceClient.DEFAULT_CONNECTION_LIMIT_PER_TAG);
    }

    public void setRequestPayloadSizeLimit(int requestPayloadSizeLimit) {
        this.requestPayloadSizeLimit = requestPayloadSizeLimit;
    }

    public int getRequestPayloadSizeLimit() {
        return this.requestPayloadSizeLimit;
    }

    private NettyChannelGroup getChannelGroup(String tag, String host, int port) {
        NettyChannelGroupKey key = new NettyChannelGroupKey(tag, host, port, this.isHttp2Only);
        return getChannelGroup(key);
    }

    private NettyChannelGroup getChannelGroup(NettyChannelGroupKey key) {
        NettyChannelGroup group;
        synchronized (this.channelGroups) {
            group = this.channelGroups.get(key);
            if (group == null) {
                group = new NettyChannelGroup(key);
                this.channelGroups.put(key, group);
            }
        }
        return group;
    }

    public long getPendingRequestCount(Operation op) {
        NettyChannelGroup group = getChannelGroup(op.getConnectionTag(), op.getUri().getHost(), op
                .getUri()
                .getPort());
        return group.pendingRequests.size();
    }

    public void connectOrReuse(NettyChannelGroupKey key, Operation request) {

        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }

        if (key == null) {
            request.fail(new IllegalArgumentException("connection key is required"));
            return;
        }

        try {
            NettyChannelGroup group = getChannelGroup(key);
            final NettyChannelContext context = selectContext(request, group);

            if (context == null) {
                // We have no available connections, request has been queued
                return;
            }

            // If the connection is open, send immediately
            if (context.getChannel() != null) {
                context.setOperation(request);
                request.complete();
                return;
            }

            // Connect, then wait for the connection to complete before either
            // sending data (HTTP/1.1) or negotiating settings (HTTP/2)
            ChannelFuture connectFuture = this.bootStrap.connect(key.host, key.port);
            connectFuture.addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception {

                    if (future.isSuccess()) {
                        Channel channel = future.channel();
                        if (NettyChannelPool.this.isHttp2Only) {
                            // We tell the channel what its channel context is, so we can use it
                            // later to manage the mapping between streams and operations
                            channel.attr(NettyChannelContext.CHANNEL_CONTEXT_KEY).set(context);

                            // We also note that this is an HTTP2 channel--it simplifies some other code
                            channel.attr(NettyChannelContext.HTTP2_KEY).set(true);
                            waitForSettings(channel, context, request, group);
                        } else {
                            context.setOpenInProgress(false);
                            context.setChannel(channel).setOperation(request);
                            sendAfterConnect(channel, context, request, null);
                        }
                    } else {
                        returnOrClose(context, true);
                        fail(request, future.cause());
                    }
                }

            });

        } catch (Throwable e) {
            fail(request, e);
        }
    }

    /**
     * Count how many HTTP/2 contexts we have. There may be more than one if we have
     * an exhausted connection that hasn't been cleaned up yet.
     * This is intended for infrastructure test purposes.
     */
    public int getHttp2ActiveContextCount(String tag, String host, int port) {
        if (!this.isHttp2Only) {
            throw new IllegalStateException(
                    "Internal error: can't get HTTP/2 information about HTTP/1 context");
        }
        NettyChannelGroup group = getChannelGroup(tag, host, port);
        return group.inUseChannels.size();
    }

    /**
     * Find the first valid HTTP/2 context that is being used to talk to a given host.
     * This is intended for infrastructure test purposes.
     */
    public NettyChannelContext getFirstValidHttp2Context(String tag, String host, int port) {
        if (!this.isHttp2Only) {
            throw new IllegalStateException(
                    "Internal error: can't get HTTP/2 information about HTTP/1 context");
        }

        NettyChannelGroup group = getChannelGroup(tag, host, port);
        NettyChannelContext context = selectHttp2Context(null, group, "");
        return context;
    }

    private NettyChannelContext selectContext(Operation op, NettyChannelGroup group) {
        if (this.isHttp2Only) {
            return selectHttp2Context(op, group, op.getUri().getPath());
        } else {
            return selectHttp11Context(op, group);
        }
    }

    /**
     * Normally there is only one HTTP/2 context per host/port, unlike HTTP/1, which
     * can have lots (we default to 128). However, when we exhaust the number of streams
     * available to a connection, we have to switch to a new connection: that's
     * why we have a list of contexts.
     *
     * We'll clean up the exhausted connection once it has no pending connections.
     * That happens in handleMaintenance().
     *
     * Note that this returns null if a HTTP/2 context isn't available. This
     * happens when the channel is already being opened. The caller will
     * queue the request to be sent after the connection is open.
     */
    private NettyChannelContext selectHttp2Context(Operation request, NettyChannelGroup group,
            String link) {
        NettyChannelContext context = null;
        NettyChannelContext badContext = null;
        int limit = this.getConnectionLimitPerTag(group.getKey().connectionTag);
        synchronized (group) {
            if (!group.inUseChannels.isEmpty()) {
                // Increase locality: we want to re-use a HTTP2 context, for the same target link
                int index = Math.abs(link.hashCode() % group.inUseChannels.size());
                NettyChannelContext ctx = group.inUseChannels.get(index);
                if (ctx.isValid()) {
                    context = ctx;
                } else {
                    LOGGER.info(ctx.getLargestStreamId() + ":" + group.getKey());
                }
            }

            if (context != null) {
                if (context.isOpenInProgress() || !group.pendingRequests.isEmpty()) {
                    // If the channel is being opened, indicate that caller should
                    // queue the operation to be delivered later.
                    group.pendingRequests.add(request);
                    return null;
                }
            }

            int activeChannelCount = group.inUseChannels.size();
            if (context != null && context.hasActiveStreams()
                    && activeChannelCount < limit) {
                // create a new channel, we are below limit for concurrent connections
                context = null;
            } else if (context == null) {
                // This is rare: do a search until we find a valid channel, the modulo scheme did
                // not produce a valid context
                for (NettyChannelContext ctx : group.inUseChannels) {
                    if (ctx.isValid()) {
                        context = ctx;
                        break;
                    }
                }
            }

            if (context != null && context.getChannel() != null
                    && !context.getChannel().isOpen()) {
                badContext = context;
                context = null;
            }

            if (context == null) {
                // If there was no channel, open one
                context = new NettyChannelContext(group.getKey(),
                        NettyChannelContext.Protocol.HTTP2);
                context.setOpenInProgress(true);
                group.inUseChannels.add(context);
            }
        }

        closeBadChannelContext(badContext);
        context.updateLastUseTime();
        return context;
    }

    /**
     * If there is an HTTP/1.1 context available, return it. We only send one request
     * at a time per context, so one may not be available. If one isn't, we return null
     * to indicate that the request needs to be queued to be sent later.
     */
    private NettyChannelContext selectHttp11Context(Operation request, NettyChannelGroup group) {
        NettyChannelContext context = group.availableChannels.poll();
        NettyChannelContext badContext = null;

        synchronized (group) {
            if (context == null) {
                int limit = getConnectionLimitPerTag(group.getKey().connectionTag);
                if (group.inUseChannels.size() >= limit) {
                    group.pendingRequests.add(request);
                    return null;
                }
                context = new NettyChannelContext(group.getKey(),
                        NettyChannelContext.Protocol.HTTP11);
                context.setOpenInProgress(true);
            }

            // It's possible that we've selected a channel that we think is open, but
            // it's not. If so, it's a bad context, so recreate it.
            if (context.getChannel() != null && !context.getChannel().isOpen()) {
                badContext = context;
                context = new NettyChannelContext(group.getKey(),
                        NettyChannelContext.Protocol.HTTP11);
                context.setOpenInProgress(true);
            }
            group.inUseChannels.add(context);
        }

        closeBadChannelContext(badContext);
        context.updateLastUseTime();
        return context;
    }

    private void closeBadChannelContext(NettyChannelContext badContext) {
        if (badContext == null) {
            return;
        }
        Logger.getAnonymousLogger().info(
                "replacing channel in bad state: " + badContext.toString());
        returnOrClose(badContext, true);
    }

    /**
     * When using HTTP/2, we have to wait for the settings to be negotiated before we can send
     * data. We wait for a promise that comes from the HTTP client channel pipeline
     */
    private void waitForSettings(Channel ch, NettyChannelContext contextFinal, Operation request,
            NettyChannelGroup group) {
        ChannelPromise settingsPromise = ch.attr(NettyChannelContext.SETTINGS_PROMISE_KEY).get();
        settingsPromise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future)
                    throws Exception {

                if (future.isSuccess()) {

                    // retrieve pending operations
                    List<Operation> pendingOps = new ArrayList<>();
                    synchronized (group) {
                        contextFinal.setOpenInProgress(false);
                        contextFinal.setChannel(future.channel()).setOperation(request);
                        pendingOps.addAll(group.pendingRequests);
                        group.pendingRequests.clear();
                    }

                    sendAfterConnect(future.channel(), contextFinal, request, group);

                    // trigger pending operations
                    for (Operation pendingOp : pendingOps) {
                        pendingOp.setSocketContext(contextFinal);
                        pendingOp.complete();
                    }

                } else {
                    returnOrClose(contextFinal, true);
                    fail(request, future.cause());
                }
            }
        });
    }

    /**
     * Now that the connection is open (and if using HTTP/2, settings have been negotiated), send
     * the request.
     */
    private void sendAfterConnect(Channel ch, NettyChannelContext contextFinal, Operation request,
            NettyChannelGroup group) {
        if (request.getStatusCode() < Operation.STATUS_CODE_FAILURE_THRESHOLD) {
            request.complete();
        } else {
            // The expiration tracking code runs in parallel with request connection and send. It uses two
            // passes: it first sets the status code of an expired operation to timed out, then, on the next
            // maintenance interval, calls fail. Calling fail twice on an operation is fine, but, we want to avoid
            // calling complete, while an operation is marked timed out because the nestCompletion() call
            // in connect() is not atomic: it can restore the original completion, which is the clients, and
            // call the client completion directly.
            request.fail(request.getStatusCode());
        }
    }

    private void fail(Operation request, Throwable e) {
        request.fail(e, Operation.STATUS_CODE_BAD_REQUEST);
    }

    public void returnOrClose(NettyChannelContext context, boolean isClose) {
        if (context == null) {
            return;
        }
        returnOrCloseDirect(context, isClose);
    }

    boolean isContextInUse(NettyChannelContext context) {
        if (context == null) {
            return false;
        }
        NettyChannelGroup group = this.channelGroups.get(context.getKey());
        return group != null && group.inUseChannels.contains(context);
    }

    /**
     * This is called when a request completes. It will handle closing
     * the connection if needed (e.g. if there was an error) and sending
     * pending requests
     */
    private void returnOrCloseDirect(NettyChannelContext context, boolean isClose) {
        Channel ch = context.getChannel();
        // For HTTP/2, we'll be pumping lots of data on a connection, so it's
        // okay if it's not writable: that's not an indication of a problem.
        // For HTTP/1, we're doing serial requests. At this point in the code,
        // if the connection isn't writable, it's an indication of a problem,
        // so we'll close the connection.
        if (this.isHttp2Only) {
            isClose = isClose || !ch.isOpen() || !context.isValid();
        } else {
            isClose = isClose || !ch.isWritable() || !ch.isOpen();
        }
        NettyChannelGroup group = this.channelGroups.get(context.getKey());
        if (group == null) {
            LOGGER.warning("Cound not find group for " + context.getKey());
            context.close();
            return;
        }

        returnOrCloseDirect(context, group, isClose);
    }

    /**
     * The implementation for returnOrCloseDirect when using HTTP/1.1
     */
    private void returnOrCloseDirect(NettyChannelContext context, NettyChannelGroup group,
            boolean isClose) {
        Operation pendingOp = null;
        synchronized (group) {
            pendingOp = group.pendingRequests.poll();
            if (isClose) {
                group.inUseChannels.remove(context);
            } else if (!this.isHttp2Only) {
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
            connectOrReuse(context.getKey(), pendingOp);
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
                        c.close(true);
                    }
                    for (NettyChannelContext c : g.inUseChannels) {
                        c.close(true);
                    }
                    g.availableChannels.clear();
                    g.inUseChannels.clear();
                }
            }
            this.eventGroup.shutdownGracefully();
            if (this.nettyExecutorService != null) {
                this.nettyExecutorService.shutdown();
            }
        } catch (Throwable e) {
            // ignore exception
        }
        this.bootStrap = null;
    }

    public void handleMaintenance(Operation op) {
        long now = Utils.getNowMicrosUtc();
        if (this.isHttp2Only) {
            handleHttp2Maintenance(now);
        } else {
            handleHttp1Maintenance(now);
        }
        op.complete();
    }

    private void handleHttp1Maintenance(long now) {
        for (NettyChannelGroup g : this.channelGroups.values()) {
            closeIdleChannelContexts(g, false, now);
        }
    }

    private void handleHttp2Maintenance(long now) {
        for (NettyChannelGroup g : this.channelGroups.values()) {
            closeInvalidHttp2ChannelContexts(g, now);
        }
    }

    /**
     * Scan unused HTTP/1.1 contexts and close any that have been unused for CHANNEL_EXPIRATION_MICROS
     */
    private void closeIdleChannelContexts(NettyChannelGroup group,
            boolean forceClose, long now) {
        synchronized (group) {
            Iterator<NettyChannelContext> it = group.availableChannels.iterator();
            while (it.hasNext()) {
                NettyChannelContext c = it.next();
                if (!forceClose) {
                    long delta = now - c.getLastUseTimeMicros();
                    if (delta < CHANNEL_EXPIRATION_MICROS) {
                        continue;
                    }
                    try {
                        if (c.getChannel() == null || !c.getChannel().isOpen()) {
                            continue;
                        }
                    } catch (Throwable e) {
                    }
                }

                it.remove();
                LOGGER.info("Closing expired channel " + c.getKey());
                c.close();
            }
        }
    }

    /**
     * Close the HTTP/2 context if it's been idle too long or if we've exhausted
     * the maximum number of streams that can be sent on the connection.
     * @param group
     */
    private void closeInvalidHttp2ChannelContexts(NettyChannelGroup group, long now) {
        synchronized (group) {
            Iterator<NettyChannelContext> it = group.inUseChannels.iterator();
            while (it.hasNext()) {
                NettyChannelContext http2Channel = it.next();
                // We close a channel for two reasons:
                // First, if it hasn't been used for a while
                // Second, if we've exhausted the number of streams
                Channel channel = http2Channel.getChannel();
                if (channel == null) {
                    continue;
                }

                if (http2Channel.hasActiveStreams()) {
                    continue;
                }

                long delta = now - http2Channel.getLastUseTimeMicros();
                if (delta < CHANNEL_EXPIRATION_MICROS && http2Channel.isValid()) {
                    continue;
                }

                it.remove();
                http2Channel.close();
            }
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
