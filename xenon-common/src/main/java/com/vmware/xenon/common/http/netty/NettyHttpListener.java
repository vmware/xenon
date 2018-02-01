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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;

import com.vmware.xenon.common.NamedThreadFactory;
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
    private AtomicInteger activeChannelCount = new AtomicInteger();
    private int port;
    private ServiceHost host;
    private Channel serverChannel;
    private Map<String, NettyListenerChannelContext> pausedChannels = new ConcurrentSkipListMap<>();
    private NioEventLoopGroup eventLoopGroup;
    private ExecutorService nettyExecutorService;
    private SslContext sslContext;
    private boolean secureAuthCookie;
    private ChannelHandler childChannelHandler;
    private boolean isListening;
    private int responsePayloadSizeLimit = RESPONSE_PAYLOAD_SIZE_LIMIT;
    private CorsConfig corsConfig;

    public NettyHttpListener(ServiceHost host) {
        this.host = host;
    }

    @Override
    public long getActiveClientCount() {
        return this.activeChannelCount.get();
    }

    @Override
    public int getPort() {
        return this.port;
    }

    public void setChildChannelHandler(ChannelHandler handler) {
        this.childChannelHandler = handler;
    }

    @Override
    public void start(int port, String bindAddress) throws Exception {
        this.nettyExecutorService = Executors.newFixedThreadPool(EVENT_LOOP_THREAD_COUNT,
                new NamedThreadFactory(this.host.getUri() + "/netty-listener"));

        this.eventLoopGroup = new NioEventLoopGroup(EVENT_LOOP_THREAD_COUNT, this.nettyExecutorService);
        if (this.childChannelHandler == null) {
            this.childChannelHandler = new NettyHttpServerInitializer(this, this.host,
                    this.sslContext, this.responsePayloadSizeLimit, this.secureAuthCookie, this.corsConfig);
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

        MaintenanceProxyService.start(this.host, this::handleMaintenance);
    }

    void addChannel(Channel c) {
        this.activeChannelCount.incrementAndGet();
    }

    void removeChannel(Channel c) {
        this.pausedChannels.remove(c.id().toString());
        this.activeChannelCount.decrementAndGet();
    }

    void pauseChannel(Channel c) {
        NettyListenerChannelContext ctx = new NettyListenerChannelContext();
        ctx.setChannel(c);
        this.host.log(Level.INFO, "Disabling auto-reads on %s", c);
        c.config().setAutoRead(false);
        ctx.updateLastUseTime();
        this.pausedChannels.put(c.id().toString(), ctx);
    }

    @Override
    public void handleMaintenance(Operation op) {
        if (this.pausedChannels.isEmpty()) {
            op.complete();
            return;
        }
        try {
            long now = Utils.getSystemNowMicrosUtc();
            for (NettyListenerChannelContext ctx : this.pausedChannels.values()) {
                Channel c = ctx.getChannel();
                if (c.config().isAutoRead()) {
                    continue;
                }

                if (now - ctx.getLastUseTimeMicros() < this.host.getMaintenanceIntervalMicros()) {
                    continue;
                }

                this.host.log(Level.INFO, "Resuming paused channel %s, last use: %d",
                        c,
                        ctx.getLastUseTimeMicros());
                c.config().setAutoRead(true);
            }
            op.complete();
        } catch (Exception e) {
            op.fail(e);
        }
    }

    @Override
    public void stop() throws IOException {
        this.isListening = false;
        this.pausedChannels.clear();
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
    public void setSSLContextFiles(URI certFile, URI keyFile) throws Exception {
        setSSLContextFiles(certFile, keyFile, null);
    }

    @Override
    public void setSSLContextFiles(URI certFile, URI keyFile, String keyPassphrase) throws Exception {
        if (isListening()) {
            throw new IllegalStateException("listener already started");
        }

        SslContextBuilder builder = SslContextBuilder.forServer(new File(certFile),
                new File(keyFile), keyPassphrase);

        if (NettyChannelContext.isALPNEnabled()) {
            builder.ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(
                            ApplicationProtocolConfig.Protocol.ALPN,
                            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2,
                            ApplicationProtocolNames.HTTP_1_1));
        }

        this.sslContext = builder.build();
    }

    @Override
    public boolean isSSLConfigured() {
        return this.sslContext != null;
    }

    @Override
    public boolean isListening() {
        return this.isListening;
    }

    @Override
    public void setResponsePayloadSizeLimit(int responsePayloadSizeLimit) {
        if (isListening()) {
            throw new IllegalStateException("Already started listening");
        }
        this.responsePayloadSizeLimit = responsePayloadSizeLimit;
    }

    @Override
    public int getResponsePayloadSizeLimit() {
        return this.responsePayloadSizeLimit;
    }

    @Override
    public boolean getSecureAuthCookie() {
        return this.secureAuthCookie;
    }

    @Override
    public void setSecureAuthCookie(boolean secureAuthCookie) {
        if (isListening()) {
            throw new IllegalStateException("Already started listening");
        }
        this.secureAuthCookie = secureAuthCookie;
    }

    /**
     * CORS configuration for netty pipeline.
     *
     * By default, CORS support is disabled.
     * To enable CORS, set {@link CorsConfig} to this method.
     *
     * Example of {@link CorsConfig}:
     * {@code
     *   // allow PUT for any origin
     *   CorsConfigBuilder.forAnyOrigin().allowedRequestMethods(HttpMethod.PUT).build();
     * }
     *
     * @see io.netty.handler.codec.http.cors.CorsConfigBuilder
     * @see io.netty.handler.codec.http.cors.CorsConfig
     */
    public void setCorsConfig(CorsConfig corsConfig) {
        if (isListening()) {
            throw new IllegalStateException("listener already started");
        }
        this.corsConfig = corsConfig;
    }

}
