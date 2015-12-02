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

import java.util.logging.Level;

import javax.net.ssl.SSLEngine;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.ssl.SslHandler;

import com.vmware.xenon.common.Operation.SocketContext;
import com.vmware.xenon.common.Utils;

public class NettyHttpClientRequestInitializer extends ChannelInitializer<SocketChannel> {

    private static final String AGGREGATOR_HANDLER = "aggregator";
    private static final String DCP_HANDLER = "dcp";
    private static final String HTTP1_CODEC = "http1-codec";
    private static final String SSL_HANDLER = "ssl";
    private static final String EVENT_LOGGER = "event-logger";

    private final NettyChannelPool pool;
    private boolean forceHttp2 = false;

    public NettyHttpClientRequestInitializer(
            NettyChannelPool nettyChannelPool,
            boolean forceHttp2) {
        this.pool = nettyChannelPool;
        this.forceHttp2 = forceHttp2;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        ch.config().setAllocator(NettyChannelContext.ALLOCATOR);
        ch.config().setSendBufferSize(NettyChannelContext.BUFFER_SIZE);
        ch.config().setReceiveBufferSize(NettyChannelContext.BUFFER_SIZE);
        if (this.pool.getSSLContext() != null) {
            if (this.forceHttp2) {
                throw new IllegalArgumentException("HTTP/2 with SSL is not supported");
            }
            SSLEngine engine = this.pool.getSSLContext().createSSLEngine();
            engine.setUseClientMode(true);
            p.addLast(SSL_HANDLER, new SslHandler(engine));
        }

        HttpClientCodec http1_codec = new HttpClientCodec(
                NettyChannelContext.MAX_INITIAL_LINE_LENGTH,
                NettyChannelContext.MAX_HEADER_SIZE,
                NettyChannelContext.MAX_CHUNK_SIZE, false);

        // The HttpClientCodec combines the HttpRequestEncoder and the HttpResponseDecoder, and it
        // also provides a method for upgrading the protocol, which we use to support HTTP/2.
        p.addLast(HTTP1_CODEC, http1_codec);

        if (this.forceHttp2) {
            try {
                HttpToHttp2ConnectionHandler connectionHandler = makeHttp2ConnectionHandler();
                Http2ClientUpgradeCodec upgradeCodec = new Http2ClientUpgradeCodec(
                        connectionHandler);
                HttpClientUpgradeHandler upgradeHandler = new HttpClientUpgradeHandler(
                        http1_codec,
                        upgradeCodec,
                        NettyChannelContext.MAX_CHUNK_SIZE);

                p.addLast("http2-codec", connectionHandler);
                p.addLast("upgrade-handler", upgradeHandler);
            } catch (Throwable ex) {
                Utils.log(NettyHttpClientRequestInitializer.class,
                        NettyHttpClientRequestInitializer.class.getSimpleName(),
                        Level.WARNING, "Channel Initializer exception: %s", ex);
                throw ex;
            }
        } else {
            /*
             * The HttpObjectAggregator is only for when we have chunked transfer encoding, but
             * that's not supported in HTTP/2 so we only insert it for HTTP/1.1
             */
            p.addLast(AGGREGATOR_HANDLER,
                    new HttpObjectAggregator(SocketContext.getMaxClientRequestSize()));
        }
        p.addLast(DCP_HANDLER, new NettyHttpServerResponseHandler(this.pool));
        p.addLast(EVENT_LOGGER, new UserEventLogger());
    }

    /**
     * For HTTP/2 we don't have anything as simple as the HttpClientCodec (at least, not in Netty
     * 5.0alpha 2), so we create the equivalent.
     *
     * @return
     */
    private HttpToHttp2ConnectionHandler makeHttp2ConnectionHandler() {
        Http2Connection connection = new DefaultHttp2Connection(false);
        InboundHttp2ToHttpAdapter inboundAdapter = new InboundHttp2ToHttpAdapter.Builder(connection)
                .maxContentLength(NettyChannelContext.MAX_CHUNK_SIZE)
                .propagateSettings(true)
                .build();
        DelegatingDecompressorFrameListener frameListener = new DelegatingDecompressorFrameListener(
                connection, inboundAdapter);
        Http2FrameReader frameReader = new DefaultHttp2FrameReader();
        Http2FrameWriter frameWriter = new DefaultHttp2FrameWriter();

        HttpToHttp2ConnectionHandler connectionHandler = new HttpToHttp2ConnectionHandler(
                connection,
                frameReader,
                frameWriter,
                frameListener);
        return connectionHandler;
    }

    /**
     * The
     */
    private static class UserEventLogger extends ChannelHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext context, Object eventRaw)
                throws Exception {
            if (eventRaw instanceof HttpClientUpgradeHandler.UpgradeEvent) {
                HttpClientUpgradeHandler.UpgradeEvent event = (HttpClientUpgradeHandler.UpgradeEvent) eventRaw;
                if (event == UpgradeEvent.UPGRADE_REJECTED) {
                    Utils.log(NettyHttpClientRequestInitializer.class,
                            NettyHttpClientRequestInitializer.class.getSimpleName(),
                            Level.WARNING, "Failed to upgrade to HTTP/2: throughput will be slow");
                } else {
                    Utils.log(NettyHttpClientRequestInitializer.class,
                            NettyHttpClientRequestInitializer.class.getSimpleName(),
                            Level.SEVERE, "===== ajr Event: %s", event);
                }
            }
            super.userEventTriggered(context, eventRaw);
        }
    }
}
