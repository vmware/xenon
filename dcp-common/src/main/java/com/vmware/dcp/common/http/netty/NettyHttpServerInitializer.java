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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
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

    public NettyHttpServerInitializer(ServiceHost host, SslContext sslContext) {
        this.sslContext = sslContext;
        this.host = host;
    }

    @Override
    public void initChannel(SocketChannel ch) {
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

        p.addLast(DECODER_HANDLER, new HttpRequestDecoder(
                NettyChannelContext.MAX_INITIAL_LINE_LENGTH,
                NettyChannelContext.MAX_HEADER_SIZE,
                NettyChannelContext.MAX_CHUNK_SIZE, false));
        p.addLast(ENCODER_HANDLER, new HttpResponseEncoder());
        p.addLast(AGGREGATOR_HANDLER,
                new HttpObjectAggregator(NettyChannelContext.getMaxRequestSize()));
        p.addLast(DCP_WEBSOCKET_HANDLER, new NettyWebSocketRequestHandler(this.host,
                ServiceUriPaths.CORE_WEB_SOCKET_ENDPOINT,
                ServiceUriPaths.WEB_SOCKET_SERVICE_PREFIX));
        p.addLast(DCP_HANDLER, new NettyHttpClientRequestHandler(this.host));
    }
}
