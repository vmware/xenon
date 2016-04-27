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
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
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
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Asynchronous connection management pool
 */
public class NettyChannelPool {

    public static class NettyChannelGroup {
        // available and inUse channels are for when we have an HTTP/1.1 connection
        // while the http2Channels are for an HTTP/2 channel. We could reuse available
        // channels for both, but this keeps it a bit more clear.
        public List<NettyChannelContext> availableChannels = new ArrayList<>();
        public Set<NettyChannelContext> inUseChannels = new HashSet<>();

        // In general, we're only using a single http2Channel at a time, but
        // we make a new channel when the existing channel has exhausted it's
        // streams (stream identifiers can't be reused). Typically, this list
        // will have a single channel in it, and will briefly have two channels
        // when switching to a new channel: we need to wait for pending operations
        // to complete before we close the exhausted channel.
        public List<NettyChannelContext> http2Channels = new ArrayList<>();
        public List<Operation> pendingRequests = new ArrayList<>();
    }

    private static final Logger logger = Logger.getLogger(NettyChannelPool.class.getName());

    private static final long CHANNEL_EXPIRATION_MICROS =
            ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS * 2;

    public static String toConnectionKey(String host, int port) {
        return host + port;
    }

    private final ExecutorService executor;
    private ExecutorService nettyExecutorService;
    private EventLoopGroup eventGroup;
    private String threadTag = NettyChannelPool.class.getSimpleName();
    private int threadCount;
    private boolean isHttp2Only = false;

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

    public void connectOrReuse(String host, int port, Operation request) {

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
            final NettyChannelContext context = selectContext(group, host, port, key);

            if (context == null) {
                // We have no available connections, so queue the request.
                synchronized (group) {
                    group.pendingRequests.add(request);
                }
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
            ChannelFuture connectFuture = this.bootStrap.connect(context.host, context.port);
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
    public int getHttp2ActiveContextCount(String host, int port) {
        if (!this.isHttp2Only) {
            throw new IllegalStateException(
                    "Internal error: can't get HTTP/2 information about HTTP/1 context");
        }

        String key = toConnectionKey(host, port);
        NettyChannelGroup group = getChannelGroup(key);
        return group.http2Channels.size();
    }

    /**
     * Find the first valid HTTP/2 context that is being used to talk to a given host.
     * This is intended for infrastructure test purposes.
     */
    public NettyChannelContext getFirstValidHttp2Context(String host, int port) {
        if (!this.isHttp2Only) {
            throw new IllegalStateException(
                    "Internal error: can't get HTTP/2 information about HTTP/1 context");
        }

        String key = toConnectionKey(host, port);
        NettyChannelGroup group = getChannelGroup(key);
        NettyChannelContext context = selectHttp2Context(group, host, port, key);
        return context;
    }

    private NettyChannelContext selectContext(NettyChannelGroup group, String host, int port, String key) {
        if (this.isHttp2Only) {
            return selectHttp2Context(group, host, port, key);
        } else {
            return selectHttp11Context(group, host, port, key);
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
    private NettyChannelContext selectHttp2Context(
            NettyChannelGroup group, String host, int port, String key) {
        NettyChannelContext http2Channel = null;
        NettyChannelContext badHttp2Channel = null;
        synchronized (group) {
            // Find a channel that's not exhausted, if any.
            for (NettyChannelContext channel : group.http2Channels) {
                if (channel.isValid()) {
                    http2Channel = channel;
                    break;
                }
            }
            if (http2Channel != null && http2Channel.getOpenInProgress()) {
                // If the channel is being opened, indicate that caller should
                // queue the operation to be delivered later.
                return null;
            }
            if (http2Channel != null && !group.pendingRequests.isEmpty()) {
                // Queue behind pending requests
                return null;
            }

            if (http2Channel == null) {
                // If there was no channel, open one
                http2Channel = new NettyChannelContext(host, port, key,
                        NettyChannelContext.Protocol.HTTP2);
                http2Channel.setOpenInProgress(true);
                group.http2Channels.add(http2Channel);
            } else if (http2Channel.getChannel() != null
                    && !http2Channel.getChannel().isOpen()) {
                badHttp2Channel = http2Channel;
                http2Channel = new NettyChannelContext(host, port, key,
                        NettyChannelContext.Protocol.HTTP2);
                http2Channel.setOpenInProgress(true);
                group.http2Channels.add(http2Channel);
            }
            http2Channel.updateLastUseTime();
        }

        if (badHttp2Channel != null) {
            Logger.getAnonymousLogger().warning(
                    "Found channel in bad state: " + badHttp2Channel.getChannel());
            // We found an channel, but it's in a bad state. Replace it.
            returnOrCloseDirectHttp2(badHttp2Channel, group, true);
        }

        return http2Channel;
    }

    /**
     * If there is an HTTP/1.1 context available, return it. We only send one request
     * at a time per context, so one may not be available. If one isn't, we return null
     * to indicate that the request needs to be queued to be sent later.
     */
    private NettyChannelContext selectHttp11Context(
            NettyChannelGroup group, String host, int port, String key) {
        NettyChannelContext context = null;

        synchronized (group) {
            if (!group.availableChannels.isEmpty()) {
                context = group.availableChannels
                        .remove(group.availableChannels.size() - 1);
                context.updateLastUseTime();
            } else if (group.inUseChannels.size() >= this.connectionLimit) {
                return null;
            } else {
                context = new NettyChannelContext(host, port, key,
                        NettyChannelContext.Protocol.HTTP11);
            }

            // It's possible that we've selected a channel that we think is open, but
            // it's not. If so, it's a bad context, so recreate it.
            if (context.getChannel() != null) {
                if (!context.getChannel().isOpen()) {
                    context.close();
                    context = new NettyChannelContext(host, port, key,
                            NettyChannelContext.Protocol.HTTP11);
                }
            }

            group.inUseChannels.add(context);
        }
        return context;

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
        if (this.isHttp2Only) {
            contextFinal.setOpenInProgress(false);
        }
        contextFinal.setChannel(ch).setOperation(request);
        request.complete();
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
        Operation pendingOp = null;
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
            connectOrReuse(context.host, context.port, pendingOp);
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

        boolean havePending;
        synchronized (group) {
            if (isClose) {
                context.setOpenInProgress(false);
                group.http2Channels.remove(context);
            }
            havePending = !group.pendingRequests.isEmpty();
        }

        if (isClose) {
            context.close();
        }

        if (!havePending) {
            return;
        }

        Operation pendingOp = null;
        if (isClose || !context.isValid()) {
            synchronized (group) {
                pendingOp = group.pendingRequests.remove(0);
            }
            connectOrReuse(context.host, context.port, pendingOp);
        } else {
            synchronized (group) {
                pendingOp = group.pendingRequests.remove(0);
            }
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
        long http1Start = 0;
        long http2Start = 0;
        long http2End = 0;
        long http1End = 0;
        if (this.isHttp2Only) {
            http2Start = now;
            handleHttp2Maintenance(op, now);
            http2End = Utils.getNowMicrosUtc();
        } else {
            http1Start = Utils.getNowMicrosUtc();
            handleHttp1Maintenance(op, now);
            http1End = Utils.getNowMicrosUtc();
        }
        long end = Utils.getNowMicrosUtc();
        if (end - now > 100) {
            String log = String.format("Duration: http2 %d, http1 %d (micros)",
                    http2End - http2Start, http1End - http1Start);
            logger.info(log);
        }
        op.complete();
    }

    private void handleHttp1Maintenance(Operation op, long nowMicros) {
        List<Operation> expiredOps = new ArrayList<>();
        for (NettyChannelGroup g : this.channelGroups.values()) {
            synchronized (g) {
                //closeContexts(g.availableChannels, nowMicros, false);
                //closeExpiredInUseContext(g.inUseChannels, nowMicros);
                //findExpiredPendingRequests(g, nowMicros, expiredOps);
            }
        }
        failExpiredRequests(expiredOps);
    }

    private void handleHttp2Maintenance(Operation op, long nowMicros) {
        List<Operation> expiredOps = new ArrayList<>();
        for (NettyChannelGroup g : this.channelGroups.values()) {
            synchronized (g) {
                findExpiredHttp2Requests(g, nowMicros, expiredOps);
                closeHttp2Context(g, nowMicros);
                findExpiredPendingRequests(g, nowMicros, expiredOps);
            }
        }
        failExpiredRequests(expiredOps);
    }

    private void failExpiredRequests(List<Operation> expiredOps) {
        if (expiredOps.isEmpty()) {
            return;
        }

        logger.info(String.format("Failing %d expired operations", expiredOps.size()));
        for (Operation op : expiredOps) {
            this.executor.execute(() -> {
                failRequestWithTimeout(op);
            });
        }
    }

    private void findExpiredPendingRequests(NettyChannelGroup g, long nowMicros,
            List<Operation> expiredOps) {
        if (g.pendingRequests.isEmpty()) {
            return;
        }
        // Pending requests are in FIFO order, and we make the simplifying assumption
        // they use similar expiration intervals. So if we find one not yet expired, assume
        // others have not either. In the future, we might track them using
        // a sorted tree map
        if (g.pendingRequests.get(0).getExpirationMicrosUtc() > nowMicros) {
            return;
        }
        Iterator<Operation> it = g.pendingRequests.iterator();
        while (it.hasNext()) {
            Operation op = it.next();
            if (op.getExpirationMicrosUtc() > nowMicros) {
                // same assumption: if the current operation has not expired, stop iteration
                break;
            }
            it.remove();
            expiredOps.add(op);
        }
        if (expiredOps.isEmpty()) {
            return;
        }
        logger.info(String.format("Found %d pending expired operations", expiredOps.size()));
    }

    /**
     * Scan HTTP/1.1 contexts and timeout any operations that need to be timed out.
     */
    private void closeExpiredInUseContext(Collection<NettyChannelContext> contexts,
            long nowMicros) {
        for (NettyChannelContext c : contexts) {
            Operation activeOp = c.getOperation();
            if (activeOp == null || activeOp.getExpirationMicrosUtc() > nowMicros) {
                continue;
            }
            this.executor.execute(() -> {
                failRequestWithTimeout(activeOp);
            });
        }
    }

    /**
     * Scan unused HTTP/1.1 contexts and close any that have been unused for
     * CHANNEL_EXPIRATION_MICROS
     */
    private void closeContexts(Collection<NettyChannelContext> contexts,
            long nowMicros, boolean forceClose) {
        long now = Utils.getNowMicrosUtc();
        List<NettyChannelContext> items = null;
        for (NettyChannelContext c : contexts) {
            long delta = nowMicros - c.getLastUseTimeMicros();
            if (!forceClose && delta < CHANNEL_EXPIRATION_MICROS) {
                break;
            }
            try {
                if (c.getChannel() == null || !c.getChannel().isOpen()) {
                    continue;
                }
            } catch (Throwable e) {
            }
            c.close();
            if (items == null) {
                items = new ArrayList<>();
            }
            items.add(c);
        }
        long end = Utils.getNowMicrosUtc();
        if (end - now > 100) {
            logger.info(contexts.size() + " closing contexts delay " + (end - now));
        }
        if (items.isEmpty()) {
            return;
        }

        for (NettyChannelContext c : items) {
            contexts.remove(c);
        }
    }

    private List<Operation> findExpiredHttp2Requests(NettyChannelGroup group, long nowMicros,
            List<Operation> expiredOps) {
        for (NettyChannelContext c : group.http2Channels) {
            // Synchronize on the stream map: same as in NettyChannelContext
            synchronized (c.streamIdMap) {
                Iterator<Entry<Integer, Operation>> entryIt = c.streamIdMap.entrySet().iterator();
                while (entryIt.hasNext()) {
                    Entry<Integer, Operation> entry = entryIt.next();
                    if (entry.getValue().getExpirationMicrosUtc() > nowMicros) {
                        // The map is sorted in ascending order, by expiration. If we find an entry
                        // that has not expired, all other entries after it, also have not
                        break;
                    }
                    expiredOps.add(entry.getValue());
                    entryIt.remove();
                }
            }
        }
        return expiredOps;
    }

    private void failRequestWithTimeout(Operation opToExpire) {
        Throwable e = new TimeoutException(opToExpire.toString());
        opToExpire.setBodyNoCloning(
                ServiceErrorResponse.create(e, Operation.STATUS_CODE_TIMEOUT,
                        EnumSet.of(ErrorDetail.SHOULD_RETRY)));
        // client has nested completion on failure, and will close context
        opToExpire.fail(e, Operation.STATUS_CODE_TIMEOUT);
    }

    /**
     * Close the HTTP/2 context if it's been idle too long or if we've exhausted
     * the maximum number of streams that can be sent on the connection.
     * @param group
     */
    private void closeHttp2Context(NettyChannelGroup group, long nowMicros) {
        List<NettyChannelContext> items = new ArrayList<>();
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

            long delta = nowMicros - http2Channel.getLastUseTimeMicros();
            if (delta < CHANNEL_EXPIRATION_MICROS && http2Channel.isValid()) {
                continue;
            }
            if (channel.isOpen()) {
                channel.close();
            }
            items.add(http2Channel);
        }
        for (NettyChannelContext c : items) {
            group.http2Channels.remove(c);
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
