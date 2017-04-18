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
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
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
import io.netty.handler.ssl.SslContext;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationQueue;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceClient.ConnectionPoolMetrics;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Asynchronous connection management pool
 */
public class NettyChannelPool {

    static ThreadLocal<NettyChannelGroupKey> lookupChannelKeyPerThread = new ThreadLocal<NettyChannelGroupKey>() {
        @Override
        public NettyChannelGroupKey initialValue() {
            return new NettyChannelGroupKey();
        }
    };

    static NettyChannelGroupKey buildLookupKey(String tag, String host, int port, boolean isHttp2) {
        NettyChannelGroupKey key = lookupChannelKeyPerThread.get();
        return key.set(tag, host, port, isHttp2);
    }

    public static class NettyChannelGroupKey implements Comparable<NettyChannelGroupKey> {
        private static final String NO_HOST = "";
        private String connectionTag;
        private String host;
        private int port;
        private int hashcode;

        public NettyChannelGroupKey() {

        }

        NettyChannelGroupKey(NettyChannelGroupKey other) {
            this.connectionTag = other.connectionTag;
            this.host = other.host;
            this.port = other.port;
        }

        public NettyChannelGroupKey set(String tag, String host, int port, boolean isHttp2) {
            if (tag == null) {
                tag = isHttp2 ? ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT
                        : ServiceClient.CONNECTION_TAG_DEFAULT;
            }
            this.connectionTag = tag;
            this.host = host == null ? NO_HOST : host;
            if (port <= 0) {
                port = UriUtils.HTTP_DEFAULT_PORT;
            }
            this.port = port;
            this.hashcode = 0;
            return this;
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

        public NettyChannelGroup(NettyChannelGroupKey key, int queueLimit) {
            this.key = key;
            this.pendingRequests = OperationQueue.createFifo(queueLimit);
        }

        public NettyChannelGroupKey getKey() {
            return this.key;
        }

        // Available channels are for when we have an HTTP/1.1 connection
        public Queue<NettyChannelContext> availableChannels = new ConcurrentLinkedQueue<>();

        public List<NettyChannelContext> inUseChannels = new ArrayList<>();
        public OperationQueue pendingRequests;
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

    private SslContext http2SslContext;
    private SSLContext sslContext;

    private int requestPayloadSizeLimit;

    private int pendingRequestQueueLimit;

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

    public NettyChannelPool setConnectionLimitPerTag(String tag, int limit) {
        this.connectionLimitsPerTag.put(tag, limit);
        return this;
    }

    public int getConnectionLimitPerTag(String tag) {
        return this.connectionLimitsPerTag.getOrDefault(tag,
                ServiceClient.DEFAULT_CONNECTION_LIMIT_PER_TAG);
    }

    public NettyChannelPool setRequestPayloadSizeLimit(int requestPayloadSizeLimit) {
        this.requestPayloadSizeLimit = requestPayloadSizeLimit;
        return this;
    }

    public int getRequestPayloadSizeLimit() {
        return this.requestPayloadSizeLimit;
    }

    /**
     * Sets pending request limit per host connection. This should only be called
     * before the client is started, or in a test setting
     */
    public NettyChannelPool setPendingRequestQueueLimit(int limit) {
        this.pendingRequestQueueLimit = limit;
        for (NettyChannelGroup g : this.channelGroups.values()) {
            synchronized (g) {
                g.pendingRequests.setLimit(limit);
            }
        }
        return this;
    }

    public int getPendingRequestQueueLimit() {
        return this.pendingRequestQueueLimit;
    }

    private NettyChannelGroup getChannelGroup(String tag, String host, int port) {
        NettyChannelGroupKey key = buildLookupKey(tag, host, port, this.isHttp2Only);
        return getChannelGroup(key);
    }

    private NettyChannelGroup getChannelGroup(NettyChannelGroupKey threadLocalKey) {
        NettyChannelGroup group;
        synchronized (this.channelGroups) {
            group = this.channelGroups.get(threadLocalKey);
            if (group == null) {
                NettyChannelGroupKey clonedKey = new NettyChannelGroupKey(threadLocalKey);
                group = new NettyChannelGroup(clonedKey, this.pendingRequestQueueLimit);
                this.channelGroups.put(clonedKey, group);
            }
        }
        return group;
    }

    public ConnectionPoolMetrics getConnectionTagInfo(String tag) {
        ConnectionPoolMetrics tagInfo = null;
        for (NettyChannelGroup g : this.channelGroups.values()) {
            if (!tag.equals(g.key.connectionTag)) {
                continue;
            }
            if (tagInfo == null) {
                tagInfo = new ConnectionPoolMetrics();
            }
            synchronized (g) {
                tagInfo.pendingRequestCount += g.pendingRequests.size();
                tagInfo.inUseConnectionCount += g.inUseChannels.size();
                tagInfo.availableConnectionCount += g.availableChannels.size();
            }
        }
        return tagInfo;
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
                        request.setSocketContext(null);
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
                    if (request != null) {
                        queuePendingRequest(request, group);
                    }
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
     * Must be called with the group synchronized
     */
    private void queuePendingRequest(Operation request, NettyChannelGroup group) {
        if (group.pendingRequests.offer(request)) {
            return;
        }
        ForkJoinPool.commonPool().execute(() -> {
            Operation.failLimitExceeded(request,
                    ServiceErrorResponse.ERROR_CODE_CLIENT_QUEUE_LIMIT_EXCEEDED);
        });
    }

    /**
     * If there is an HTTP/1.1 context available, return it. We only send one request
     * at a time per context, so one may not be available. If one isn't, we return null
     * to indicate that the request needs to be queued to be sent later.
     */
    private NettyChannelContext selectHttp11Context(Operation request, NettyChannelGroup group) {
        NettyChannelContext context;
        NettyChannelContext badContext = null;

        synchronized (group) {
            context = group.availableChannels.poll();
            if (context == null) {
                int limit = getConnectionLimitPerTag(group.getKey().connectionTag);
                if (group.inUseChannels.size() >= limit) {
                    queuePendingRequest(request, group);
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
                        group.pendingRequests.transferAll(pendingOps);
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

        if (ch != null) {
            if (this.isHttp2Only) {
                isClose = isClose || !ch.isOpen() || !context.isValid();
            } else {
                isClose = isClose || !ch.isWritable() || !ch.isOpen();
            }
        }

        NettyChannelGroup group = this.channelGroups.get(context.getKey());
        if (group == null) {
            LOGGER.warning("Cound not find group for " + context.getKey());
            context.close();
            return;
        }

        returnOrCloseDirect(context, group, isClose);
    }

    private void returnOrCloseDirect(NettyChannelContext context, NettyChannelGroup group,
            boolean isClose) {
        Operation pendingOp = null;
        synchronized (group) {
            pendingOp = group.pendingRequests.poll();
            if (isClose) {
                group.inUseChannels.remove(context);
            } else if (!this.isHttp2Only && pendingOp == null) {
                if (group.inUseChannels.remove(context)) {
                    group.availableChannels.add(context);
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
        long now = Utils.getSystemNowMicrosUtc();
        if (this.isHttp2Only) {
            handleHttp2Maintenance(now);
        } else {
            handleHttp1Maintenance(now);
        }
        op.complete();
    }

    private void handleHttp1Maintenance(long now) {
        for (NettyChannelGroup g : this.channelGroups.values()) {
            logGroupStatus(g);
            closeIdleChannelContexts(g, false, now);
        }
    }

    private void handleHttp2Maintenance(long now) {
        for (NettyChannelGroup g : this.channelGroups.values()) {
            logGroupStatus(g);
            closeInvalidHttp2ChannelContexts(g, now);
        }
    }

    private void logGroupStatus(NettyChannelGroup g) {
        if (!LOGGER.isLoggable(Level.FINE)) {
            return;
        }
        String s = null;
        synchronized (g) {
            s = String.format("Maintenance on %s, pending: %d, available channels: %d",
                    g.getKey(), g.pendingRequests.size(), g.availableChannels.size());
        }
        LOGGER.info(s);
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
                LOGGER.warning("Closing expired channel " + c.getKey());
                c.close();
            }
        }

        checkPendingOperations(group);
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

        checkPendingOperations(group);
    }

    private void checkPendingOperations(NettyChannelGroup group) {
        if (group.pendingRequests.isEmpty()) {
            return;
        }

        // The HTTP client is responsible for failing expired operations and maintains
        // an independent tracking list. As a defense-in-depth check however, warn when
        // operations remain in our pending list AFTER they are expired
        final int searchLimit = 1000;
        int count = 0;
        int removedCount = 0;

        synchronized (group) {
            Iterator<Operation> pendingOpIt = group.pendingRequests.iterator();
            while (pendingOpIt.hasNext() && ++count < searchLimit) {
                Operation pendingOp = pendingOpIt.next();

                if (pendingOp.getStatusCode() < Operation.STATUS_CODE_FAILURE_THRESHOLD) {
                    if (count > 10) {
                        // We are using a FIFO queue, so if oldest operations have not expired,
                        // assume no others have. This is not always true, for operations with
                        // widely different expirations, but this is defense in depth, not a primary
                        // mechanism for expiration and we want to keep the overhead small
                        break;
                    } else {
                        continue;
                    }
                }
                pendingOpIt.remove();
                removedCount++;
            }
        }

        if (removedCount == 0) {
            return;
        }

        LOGGER.warning(String.format("Pending %d, failed pending operations removed: %d",
                group.pendingRequests.size(), removedCount));
    }

    public void setHttp2SslContext(SslContext context) {
        if (isStarted()) {
            throw new IllegalStateException("Already started");
        }
        this.http2SslContext = context;
    }

    public SslContext getHttp2SslContext() {
        return this.http2SslContext;
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
