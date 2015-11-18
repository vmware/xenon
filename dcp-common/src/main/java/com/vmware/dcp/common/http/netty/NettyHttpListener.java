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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.ExecutorServiceFactory;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceHost;
import com.vmware.dcp.common.ServiceRequestListener;

/**
 * Asynchronous HTTP request listener using the Netty I/O framework. Interacts with a parent service
 * host to deliver HTTP requests from the network, to local services
 */
public class NettyHttpListener implements ServiceRequestListener {
    public static final String UNKNOWN_CLIENT_REFERER_PATH = "unknown-client";
    public static final int EVENT_LOOP_THREAD_COUNT = 2;
    private int port;
    private ServiceHost host;
    private Channel serverChannel;
    private NioEventLoopGroup eventLoopGroup;
    private SslContext sslContext;
    private ChannelHandler childChannelHandler;

    public NettyHttpListener(ServiceHost host) {
        this.host = host;
    }

    public long getActiveClientCount() {
        // TODO Add tracking of client connections by exposing a counter the
        // NettyHttpRequestHandler instance can increment/decrement
        return 0;
    }

    public int getPort() {
        return this.port;
    }

    public void setChildChannelHandler(ChannelHandler handler) {
        this.childChannelHandler = handler;
    }

    public void start(int port, String bindAddress) throws Throwable {
        ExecutorServiceFactory f = (tc) -> {
            return Executors.newFixedThreadPool(EVENT_LOOP_THREAD_COUNT,
                    r -> new Thread(r, this.host.getUri().toString() + "/netty-listener/"
                            + this.host.getId()));
        };

        this.eventLoopGroup = new NioEventLoopGroup(EVENT_LOOP_THREAD_COUNT, f);
        if (this.childChannelHandler == null) {
            this.childChannelHandler = new NettyHttpServerInitializer(this.host, this.sslContext);
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(this.eventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(this.childChannelHandler);

        InetSocketAddress addr = null;
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
    }

    @Override
    public void handleMaintenance(Operation op) {
        op.complete();
    }

    public void stop() throws IOException {
        if (this.serverChannel == null) {
            return;
        }

        this.serverChannel.close();
        this.eventLoopGroup.shutdownGracefully();
        this.serverChannel = null;
    }

    @Override
    public void setSSLContextFiles(URI certFile, URI keyFile) throws Throwable {
        this.sslContext = SslContext.newServerContext(new File(certFile), new File(keyFile));
    }

    @Override
    public boolean isSSLConfigured() {
        return this.sslContext != null;
    }

}