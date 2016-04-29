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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Asynchronous connection management pool
 */
public class NettyChannelPool {

    public static class NettyChannelGroupKey implements Comparable<NettyChannelGroupKey> {
        public static final String DEFAULT_TAG = "";
        private final String connectionTag;
        private final String host;
        private final int port;
        private int hashcode;


        public NettyChannelGroupKey(String tag, String host, int port) {
            if (tag == null) {
                this.connectionTag = DEFAULT_TAG;
            } else {
                this.connectionTag = tag;
            }
            this.host = host;
            if (port <= 0) {
                port = UriUtils.HTTP_DEFAULT_PORT;
            }
            this.port = port;
        }

        @Override
        public int hashCode() {
            if (this.hashcode == 0) {
                this.hashcode = this.connectionTag.hashCode() ^ this.host.hashCode() ^ this.port;
            }
            return this.hashcode;
        }

        @Override
        public int compareTo(NettyChannelGroupKey o) {
            int r = this.connectionTag.compareTo(o.connectionTag);
            if (r != 0) {
                return r;
            }
            r = this.host.compareTo(o.host);
            if (r != 0) {
                return r;
            }
            return Integer.compare(this.port, o.port);
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

        // available and inUse channels are for when we have an HTTP/1.1 connection
        // while the http2Channels are for an HTTP/2 channel. We could reuse available
        // channels for both, but this keeps it a bit more clear.
        public Queue<NettyChannelContext> availableChannels = new ConcurrentLinkedQueue<>();
        public Set<NettyChannelContext> inUseChannels = new ConcurrentSkipListSet<>();

        // In general, we're only using a single http2Channel at a time, but
        // we make a new channel when the existing channel has exhausted it's
        // streams (stream identifiers can't be reused). Typically, this list
        // will have a single channel in it, and will briefly have two channels
        // when switching to a new channel: we need to wait for pending operations
        // to complete before we close the exhausted channel.
        public Set<NettyChannelContext> http2Channels = new ConcurrentSkipListSet<>();
        public Queue<Operation> pendingRequests = new ConcurrentLinkedQueue<>();
    }

    private static final long CHANNEL_EXPIRATION_MICROS =
            ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS * 2;

    private ExecutorService nettyExecutorService;
    private EventLoopGroup eventGroup;
    private String threadTag = NettyChannelPool.class.getSimpleName();
    private int threadCount;
    private boolean isHttp2Only = false;

    private Bootstrap bootStrap;

    private final Map<NettyChannelGroupKey, NettyChannelGroup> channelGroups = new ConcurrentSkipListMap<>();
    private int connectionLimit = 1;

    private SSLContext sslContext;

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

        this.nettyExecutorService = Executors.newFixedThreadPool(this.threadCount, r -> new Thread(r, this.threadTag));
        this.eventGroup = new NioEventLoopGroup(this.threadCount, this.nettyExecutorService);

        this.bootStrap = new Bootstrap();
        this.bootStrap.group(this.eventGroup)
                .channel(NioSocketChannel.class)
                .handler(new NettyHttpClientRequestInitializer(this, this.isHttp2Only));
    }

    public boolean isStarted() {
        return this.bootStrap != null;
    }

    /**
     * For an HTTP/1.1 connection, the number of actual connections per host
     * For an HTTP/2 connection, the number of streams per connection. (We have one connection
     *   per host)
     * @param limit
     * @return
     */
    public NettyChannelPool setConnectionLimitPerHost(int limit) {
        this.connectionLimit = limit;
        return this;
    }

    public int getConnectionLimitPerHost() {
        return this.connectionLimit;
    }

    private NettyChannelGroup getChannelGroup(String tag, String host, int port) {
        NettyChannelGroupKey key = new NettyChannelGroupKey(tag, host, port);
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
            final NettyChannelContext context = selectContext(group);

            if (context == null) {
                // We have no available connections, so queue the request.
                group.pendingRequests.add(request);
                return;
            }

            // If the connection is open, send immediately
            if (context.getChannel() != null) {
                context.setOperation(request);
                request.complete();
                return;
            }

            // Sometimes when an HTTP/2 connection is exhausted and we open
            // a new connection, the connection fails: it appears that the client
            // believes it has sent the HTTP/2 settings frame, but the server has
            // not received it. After hours of debugging, I don't believe the cause
            // is our fault, but haven't isolated an underlying bug in Netty either.
            //
            // The workaround is that retry the connection. I haven't yet seen a failure
            // when we retry.
            //
            // This doesn't make me completely comfortable. Is it really the case that
            // we just occasionally lose the SETTINGS frame on a new connection, or can
            // other frames be lost? Until we are sure, HTTP/2 support should be considered
            // experimental.
            if (this.isHttp2Only && request.getRetryCount() == 0) {
                request.setRetryCount(2);
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
        return group.http2Channels.size();
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
        NettyChannelContext context = selectHttp2Context(group);
        return context;
    }

    private NettyChannelContext selectContext(NettyChannelGroup group) {
        if (this.isHttp2Only) {
            return selectHttp2Context(group);
        } else {
            return selectHttp11Context(group);
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
    private NettyChannelContext selectHttp2Context(NettyChannelGroup group) {
        NettyChannelContext context = null;
        NettyChannelContext badContext = null;
        synchronized (group) {
            // Find a channel that's not exhausted, if any.
            for (NettyChannelContext channel : group.http2Channels) {
                if (channel.isValid()) {
                    context = channel;
                    break;
                }
            }
            if (context != null && context.isOpenInProgress()) {
                // If the channel is being opened, indicate that caller should
                // queue the operation to be delivered later.
                return null;
            }
            if (context != null && !group.pendingRequests.isEmpty()) {
                // Queue behind pending requests
                return null;
            }

            if (context == null) {
                // If there was no channel, open one
                context = new NettyChannelContext(group.getKey(),
                        NettyChannelContext.Protocol.HTTP2);
                context.setOpenInProgress(true);
                group.http2Channels.add(context);
            } else if (context.getChannel() != null
                    && !context.getChannel().isOpen()) {
                badContext = context;
                context = new NettyChannelContext(group.getKey(),
                        NettyChannelContext.Protocol.HTTP2);
                context.setOpenInProgress(true);
                group.http2Channels.add(context);
            }
            context.updateLastUseTime();
        }

        closeBadChannelContext(badContext);
        return context;
    }

    /**
     * If there is an HTTP/1.1 context available, return it. We only send one request
     * at a time per context, so one may not be available. If one isn't, we return null
     * to indicate that the request needs to be queued to be sent later.
     */
    private NettyChannelContext selectHttp11Context(NettyChannelGroup group) {
        NettyChannelContext context = group.availableChannels.poll();
        NettyChannelContext badContext = null;

        if (context == null) {
            synchronized (group) {
                if (group.inUseChannels.size() >= this.connectionLimit) {
                    return null;
                }
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
        }
        context.updateLastUseTime();
        group.inUseChannels.add(context);

        closeBadChannelContext(badContext);
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
                    sendAfterConnect(future.channel(), contextFinal, request, group);

                    // retrieve pending operations
                    List<Operation> pendingOps = new ArrayList<>();
                    synchronized (group) {
                        pendingOps.addAll(group.pendingRequests);
                        group.pendingRequests.clear();
                    }

                    // trigger pending operations
                    for (Operation pendingOp : pendingOps) {
                        contextFinal.setOperation(pendingOp);
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
        contextFinal.setOpenInProgress(false);
        contextFinal.setChannel(ch).setOperation(request);
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
            isClose = isClose || !ch.isOpen();
        } else {
            isClose = isClose || !ch.isWritable() || !ch.isOpen();
        }
        NettyChannelGroup group = this.channelGroups.get(context.getKey());
        if (group == null) {
            context.close();
            return;
        }

        if (this.isHttp2Only) {
            returnOrCloseDirectHttp2(context, group, isClose);
        } else {
            returnOrCloseDirectHttp1(context, group, isClose);
        }
    }

    /**
     * The implementation for returnOrCloseDirect when using HTTP/1.1
     */
    private void returnOrCloseDirectHttp1(NettyChannelContext context, NettyChannelGroup group,
            boolean isClose) {
        Operation pendingOp = group.pendingRequests.poll();
        synchronized (group) {
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
            connectOrReuse(context.getKey(), pendingOp);
        } else {
            context.setOperation(pendingOp);
            pendingOp.complete();
        }
    }

    /**
     * The implementation for returnOrCloseDirect when using HTTP/2
     */
    private void returnOrCloseDirectHttp2(NettyChannelContext context, NettyChannelGroup group,
            boolean isClose) {

        Operation pendingOp = group.pendingRequests.poll();
        if (isClose) {
            context.setOpenInProgress(false);
            group.http2Channels.remove(context);
            context.close();
        }

        if (pendingOp == null) {
            return;
        }

        if (isClose || !context.isValid()) {
            connectOrReuse(context.getKey(), pendingOp);
        } else {
            pendingOp.setSocketContext(context);
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
                    for (NettyChannelContext c : g.http2Channels) {
                        c.close();
                    }
                    g.availableChannels.clear();
                    g.inUseChannels.clear();
                    g.http2Channels.clear();
                }
            }
            this.eventGroup.shutdownGracefully();
            this.nettyExecutorService.shutdown();
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
            closeIdleHttp2ChannelsContexts(g, now);
        }
    }

    /**
     * Scan unused HTTP/1.1 contexts and close any that have been unused for CHANNEL_EXPIRATION_MICROS
     */
    private void closeIdleChannelContexts(NettyChannelGroup group,
            boolean forceClose, long now) {
        List<NettyChannelContext> items = null;
        for (NettyChannelContext c : group.availableChannels) {
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

            if (items == null) {
                items = new ArrayList<>();
            }
            items.add(c);
        }
        if (items == null) {
            return;
        }
        for (NettyChannelContext c : items) {
            if (!group.availableChannels.remove(c)) {
                continue;
            }
            c.close();
        }
    }

    /**
     * Close the HTTP/2 context if it's been idle too long or if we've exhausted
     * the maximum number of streams that can be sent on the connection.
     * @param group
     */
    private void closeIdleHttp2ChannelsContexts(NettyChannelGroup group, long now) {
        List<NettyChannelContext> items = null;
        for (NettyChannelContext http2Channel : group.http2Channels) {
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

            if (items == null) {
                items = new ArrayList<>();
            }
            items.add(http2Channel);
        }

        if (items == null) {
            return;
        }
        for (NettyChannelContext c : items) {
            if (!group.http2Channels.remove(c)) {
                return;
            }
            c.close();
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
