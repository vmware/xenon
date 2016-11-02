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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.SocketContext;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRequestListener;
import com.vmware.xenon.common.Utils;

/**
 * Asynchronous HTTP request listener using the Netty I/O framework. Interacts with a parent service
 * host to deliver HTTP requests from the network, to local services
 */
public class NettyHttpListener implements ServiceRequestListener {

    public static class NettyListenerChannelContext extends SocketContext {

        private Channel channel;

        public NettyListenerChannelContext setChannel(Channel c) {
            this.channel = c;
            super.updateLastUseTime();
            return this;
        }

        public Channel getChannel() {
            return this.channel;
        }

    }

    public static final String UNKNOWN_CLIENT_REFERER_PATH = "unknown-client";
    public static final int EVENT_LOOP_THREAD_COUNT = 2;
    private int port;
    private ServiceHost host;
    private Channel serverChannel;
    private Map<String, NettyListenerChannelContext> activeChannels = new ConcurrentSkipListMap<>();
    private NioEventLoopGroup eventLoopGroup;
    private ExecutorService nettyExecutorService;
    private SslContext sslContext;
    private ChannelHandler childChannelHandler;
    private boolean isListening;
    private int responsePayloadSizeLimit = RESPONSE_PAYLOAD_SIZE_LIMIT;

    public NettyHttpListener(ServiceHost host) {
        this.host = host;
    }

    @Override
    public long getActiveClientCount() {
        return this.activeChannels.size();
    }

    @Override
    public int getPort() {
        return this.port;
    }

    public void setChildChannelHandler(ChannelHandler handler) {
        this.childChannelHandler = handler;
    }

    @Override
    public void start(int port, String bindAddress) throws Throwable {
        this.nettyExecutorService = Executors.newFixedThreadPool(EVENT_LOOP_THREAD_COUNT,
                r -> new Thread(r, this.host.getUri().toString() + "/netty-listener/"
                        + this.host.getId()));

        this.eventLoopGroup = new NioEventLoopGroup(EVENT_LOOP_THREAD_COUNT, this.nettyExecutorService);
        if (this.childChannelHandler == null) {
            this.childChannelHandler = new NettyHttpServerInitializer(this, this.host,
                    this.sslContext,
                    this.responsePayloadSizeLimit);
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(this.eventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(this.childChannelHandler);

        InetSocketAddress addr;
        if (bindAddress != null) {
            addr = new InetSocketAddress(bindAddress, port);
        } else {
            this.host.log(Level.WARNING,
                    "*** Binding to all interfaces, please supply a bindAddress instead ***");
            addr = new InetSocketAddress(port);
        }
        this.serverChannel = b.bind(addr).sync().channel();
        this.serverChannel.config().setOption(ChannelOption.SO_LINGER, 0);
        this.port = ((InetSocketAddress) this.serverChannel.localAddress()).getPort();
        this.isListening = true;
    }

    void addChannel(Channel c) {
        NettyListenerChannelContext ctx = new NettyListenerChannelContext();
        ctx.setChannel(c);
        if (null != this.activeChannels.put(c.id().toString(), ctx)) {
            this.host.log(Level.INFO, "duplicate channel : %s %d",
                    c.id().toString(),
                    this.activeChannels.size());
        }
    }

    void removeChannel(Channel c) {
        this.activeChannels.remove(c.id().toString());
    }

    void disableChannel(Channel c) {
        NettyListenerChannelContext ctx = this.activeChannels.get(c.id().toString());
        if (ctx == null) {
            this.host.log(Level.INFO, "missing channel : %s %d",
                    c.id().toString(),
                    this.activeChannels.size());
            return;
        }

        this.host.log(Level.INFO, "Operation was rate limited, disabling auto-reads on %s", c);
        c.config().setAutoRead(false);
        ctx.updateLastUseTime();
    }

    @Override
    public void handleMaintenance(Operation op) {
        long now = Utils.getSystemNowMicrosUtc();
        for (NettyListenerChannelContext ctx : this.activeChannels.values()) {
            Channel c = ctx.getChannel();
            if (c.config().isAutoRead()) {
                continue;
            }
            this.host.log(Level.INFO, "Channel is disabled: %s, last use: %d", c,
                    ctx.getLastUseTimeMicros());
            if (now - ctx.getLastUseTimeMicros() > this.host.getMaintenanceIntervalMicros()) {
                c.config().setAutoRead(true);
            }

        }
        op.complete();
    }

    @Override
    public void stop() throws IOException {
        this.isListening = false;
        this.activeChannels.clear();
        if (this.serverChannel != null) {
            this.serverChannel.close();
            this.serverChannel = null;
        }
        if (this.eventLoopGroup != null) {
            this.eventLoopGroup.shutdownGracefully();
            this.eventLoopGroup = null;
        }
        if (this.nettyExecutorService != null) {
            this.nettyExecutorService.shutdown();
            this.nettyExecutorService = null;
        }
        this.host.setPublicUri(null);
    }

    /**
     * Sets a caller configured Netty SSL context
     */
    public void setSSLContext(SslContext context) {
        if (isListening()) {
            throw new IllegalStateException("listener already started");
        }
        this.sslContext = context;
    }

    public SslContext getSSLContext() {
        return this.sslContext;
    }

    @Override
    public void setSSLContextFiles(URI certFile, URI keyFile) throws Throwable {
        setSSLContextFiles(certFile, keyFile, null);
    }

    @Override
    public void setSSLContextFiles(URI certFile, URI keyFile, String keyPassphrase) throws Throwable {
        if (isListening()) {
            throw new IllegalStateException("listener already started");
        }
        this.sslContext = SslContextBuilder.forServer(
                new File(certFile), new File(keyFile), keyPassphrase)
                .build();
    }

    @Override
    public boolean isSSLConfigured() {
        return this.sslContext != null;
    }

    @Override
    public boolean isListening() {
        return this.isListening;
    }

    public void setResponsePayloadSizeLimit(int responsePayloadSizeLimit) {
        if (isListening()) {
            throw new IllegalStateException("Already started listening");
        }
        this.responsePayloadSizeLimit = responsePayloadSizeLimit;
    }

    public int getResponsePayloadSizeLimit() {
        return this.responsePayloadSizeLimit;
    }
}
