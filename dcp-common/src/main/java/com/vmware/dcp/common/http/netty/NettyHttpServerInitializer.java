/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

import java.util.Collections;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import com.vmware.dcp.common.ServiceHost;
import com.vmware.dcp.common.ServiceHost.ServiceHostState.SslClientAuthMode;
import com.vmware.dcp.services.common.ServiceUriPaths;

public class NettyHttpServerInitializer extends ChannelInitializer<SocketChannel> {
    public static final String AGGREGATOR_HANDLER = "aggregator";
    public static final String DCP_HANDLER = "dcp";
    public static final String DCP_WEBSOCKET_HANDLER = "dcp_ws";
    public static final String DECODER_HANDLER = "decoder";
    public static final String ENCODER_HANDLER = "encoder";
    public static final String SSL_HANDLER = "ssl";

    private final SslContext sslContext;
    private ServiceHost host;
    private boolean allowUpgrade;
    private HttpToHttp2ConnectionHandler connectionHandler;

    public NettyHttpServerInitializer(ServiceHost host, SslContext sslContext, boolean allowUpgrade) {
        this.sslContext = sslContext;
        this.host = host;
        this.allowUpgrade = allowUpgrade;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        if (!this.allowUpgrade) {
            initForNoUpgrade(ch);
        } else {
            if (this.sslContext == null) {
                initForUpgradeByClearText(ch);
            } else {
                initForUpgradeBySsl(ch);
            }
        }
    }

    private void initForNoUpgrade(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        ch.config().setAllocator(NettyChannelContext.ALLOCATOR);
        ch.config().setSendBufferSize(NettyChannelContext.BUFFER_SIZE);
        ch.config().setReceiveBufferSize(NettyChannelContext.BUFFER_SIZE);
        if (this.sslContext != null) {
            SslHandler handler = this.sslContext.newHandler(ch.alloc());
            SslClientAuthMode mode = this.host.getState().sslClientAuthMode;
            if (mode != null) {
                switch (mode) {
                case NEED:
                    handler.engine().setNeedClientAuth(true);
                    break;
                case WANT:
                    handler.engine().setWantClientAuth(true);
                    break;
                default:
                    break;
                }
            }
            p.addLast(SSL_HANDLER, handler);
        }

        p.addLast(DECODER_HANDLER, new HttpRequestDecoder());
        p.addLast(ENCODER_HANDLER, new HttpResponseEncoder());
        p.addLast(AGGREGATOR_HANDLER,
                new HttpObjectAggregator(NettyChannelContext.MAX_REQUEST_SIZE));
        p.addLast(DCP_WEBSOCKET_HANDLER, new NettyWebSocketRequestHandler(this.host,
                ServiceUriPaths.CORE_WEB_SOCKET_ENDPOINT,
                ServiceUriPaths.WEB_SOCKET_SERVICE_PREFIX));
        p.addLast(DCP_HANDLER, new NettyHttpClientRequestHandler(this.host));
    }

    private void initForUpgradeBySsl(SocketChannel ch) {
        if (this.sslContext != null) {
            SslHandler handler = this.sslContext.newHandler(ch.alloc());
            SslClientAuthMode mode = this.host.getState().sslClientAuthMode;
            if (mode != null) {
                switch (mode) {
                case NEED:
                    handler.engine().setNeedClientAuth(true);
                    break;
                case WANT:
                    handler.engine().setWantClientAuth(true);
                    break;
                default:
                    break;
                }
            }
            ch.pipeline().addLast(handler,
                    new NettyHttp2OrHttpClientRequestHandler(this.host));
        }
    }

    private void initForUpgradeByClearText(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        ch.config().setAllocator(NettyChannelContext.ALLOCATOR);
        ch.config().setSendBufferSize(NettyChannelContext.BUFFER_SIZE);
        ch.config().setReceiveBufferSize(NettyChannelContext.BUFFER_SIZE);

        DefaultHttp2Connection connection = new DefaultHttp2Connection(true);
        InboundHttp2ToHttpAdapter listener = new InboundHttp2ToHttpAdapter.Builder(connection)
                .propagateSettings(true)
                .validateHttpHeaders(false)
                .maxContentLength(NettyChannelContext.MAX_REQUEST_SIZE)
                .build();

        this.connectionHandler = new HttpToHttp2ConnectionHandler(connection, listener);

        HttpServerCodec sourceCodec = new HttpServerCodec();
        HttpServerUpgradeHandler.UpgradeCodec upgradeCodec =
                new Http2ServerUpgradeCodec(new Http2ConnectionHandler(connection, listener));

        HttpServerUpgradeHandler upgradeHandler =
                new HttpServerUpgradeHandler(sourceCodec,
                        Collections.singletonList(upgradeCodec),
                        NettyChannelContext.MAX_REQUEST_SIZE);

        p.addLast("sourcecodec", sourceCodec);
        p.addLast("upgrader", upgradeHandler);
        p.addLast("logger", new UserEventLogger());
    }

    private void configureHttp2ClearTextPipeline(ChannelHandlerContext ctx) {
        //Configures the pipeline AFTER an upgrade was successful
        ChannelPipeline p = ctx.pipeline();

        p.addAfter("upgrader", "connection", this.connectionHandler);
        p.addAfter("connection", "handler", new NettyHttpClientRequestHandler(this.host));
    }

    private class UserEventLogger extends ChannelHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            System.out.println("User Event Triggered: " + evt);

            if (evt instanceof  HttpServerUpgradeHandler.UpgradeEvent) {
                // Write an HTTP/2 response to the upgrade request
                configureHttp2ClearTextPipeline(ctx);
            }

            ctx.fireUserEventTriggered(evt);
        }
    }
}
