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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeCodec;
import io.netty.handler.codec.http.HttpServerUpgradeHandler.UpgradeCodecFactory;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2ServerUpgradeCodec;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AsciiString;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.SslClientAuthMode;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class NettyHttpServerInitializer extends ChannelInitializer<SocketChannel> {
    public static final String AGGREGATOR_HANDLER = "aggregator";
    public static final String HTTP_REQUEST_HANDLER = "http-request-handler";
    public static final String WEBSOCKET_HANDLER = "websocket-request-handler";
    public static final String HTTP1_CODEC = "http1-codec";
    public static final String HTTP2_UPGRADE_HANDLER = "http2-upgrade-handler";
    public static final String SSL_HANDLER = "ssl";

    private final SslContext sslContext;
    private ServiceHost host;
    private int responsePayloadSizeLimit;
    private static final boolean debugLogging = false;

    public NettyHttpServerInitializer(ServiceHost host, SslContext sslContext, int responsePayloadSizeLimit) {
        this.sslContext = sslContext;
        this.host = host;
        this.responsePayloadSizeLimit = responsePayloadSizeLimit;
        NettyLoggingUtil.setupNettyLogging();
    }

    /**
     * initChannel is called by Netty when a channel is first used.
     */
    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        ch.config().setAllocator(NettyChannelContext.ALLOCATOR);
        ch.config().setSendBufferSize(NettyChannelContext.BUFFER_SIZE);
        ch.config().setReceiveBufferSize(NettyChannelContext.BUFFER_SIZE);

        SslHandler sslHandler = null;
        if (this.sslContext != null) {
            sslHandler = this.sslContext.newHandler(ch.alloc());
            SslClientAuthMode mode = this.host.getState().sslClientAuthMode;
            if (mode != null) {
                switch (mode) {
                case NEED:
                    sslHandler.engine().setNeedClientAuth(true);
                    break;
                case WANT:
                    sslHandler.engine().setWantClientAuth(true);
                    break;
                default:
                    break;
                }
            }
            p.addLast(SSL_HANDLER, sslHandler);
        }

        // The HttpServerCodec combines the HttpRequestDecoder and the HttpResponseEncoder, and it
        // also provides a method for upgrading the protocol, which we use to support HTTP/2. It
        // also supports a couple other minor features (support for HEAD and CONNECT), which
        // probably don't matter to us.
        HttpServerCodec http1_codec = new HttpServerCodec(
                NettyChannelContext.MAX_INITIAL_LINE_LENGTH,
                NettyChannelContext.MAX_HEADER_SIZE,
                NettyChannelContext.MAX_CHUNK_SIZE, false);
        p.addLast(HTTP1_CODEC, http1_codec);
        if (this.sslContext == null) {
            // Today we only use HTTP/2 when SSL is disabled
            final HttpToHttp2ConnectionHandler connectionHandler = makeHttp2ConnectionHandler();
            UpgradeCodecFactory upgradeCodecFactory = new UpgradeCodecFactory() {
                @Override
                public UpgradeCodec newUpgradeCodec(CharSequence protocol) {
                    if (AsciiString.contentEquals(Http2CodecUtil.HTTP_UPGRADE_PROTOCOL_NAME,
                            protocol)) {
                        return new Http2ServerUpgradeCodec(connectionHandler);
                    } else {
                        return null;
                    }
                }
            };
            // On upgrade, the upgradeHandler will remove the http1_codec and replace it
            // with the connectionHandler. Ideally we'd remove the aggregator (chunked transfer
            // isn't allowed in HTTP/2) and the WebSocket handler (we don't support it over HTTP/2 yet)
            // but we're not doing that yet.

            HttpServerUpgradeHandler upgradeHandler = new HttpServerUpgradeHandler(
                    http1_codec, upgradeCodecFactory);

            p.addLast(HTTP2_UPGRADE_HANDLER, upgradeHandler);
        }

        p.addLast(AGGREGATOR_HANDLER,
                new HttpObjectAggregator(this.responsePayloadSizeLimit));
        p.addLast(WEBSOCKET_HANDLER, new NettyWebSocketRequestHandler(this.host,
                ServiceUriPaths.CORE_WEB_SOCKET_ENDPOINT,
                ServiceUriPaths.WEB_SOCKET_SERVICE_PREFIX));
        p.addLast(HTTP_REQUEST_HANDLER, new NettyHttpClientRequestHandler(this.host, sslHandler,
                this.responsePayloadSizeLimit));
    }

    /**
     * For HTTP/2 we don't have anything as simple as the HttpServerCodec (at least, not in Netty
     * 5.0alpha 2), so we create the equivalent.
     *
     * @return
     */
    private HttpToHttp2ConnectionHandler makeHttp2ConnectionHandler() {
        // DefaultHttp2Connection is for client or server. True means "server".
        Http2Connection connection = new DefaultHttp2Connection(true);
        InboundHttp2ToHttpAdapter inboundAdapter = new InboundHttp2ToHttpAdapterBuilder(connection)
                .maxContentLength(this.responsePayloadSizeLimit)
                .propagateSettings(false)
                .build();
        DelegatingDecompressorFrameListener frameListener = new DelegatingDecompressorFrameListener(
                connection, inboundAdapter);

        Http2Settings settings = new Http2Settings();
        //settings.maxConcurrentStreams(NettyHttpServiceClient.DEFAULT_HTTP2_STREAMS_PER_HOST);
        settings.initialWindowSize(NettyChannelContext.INITIAL_HTTP2_WINDOW_SIZE);

        HttpToHttp2ConnectionHandlerBuilder builder = new HttpToHttp2ConnectionHandlerBuilder()
                .frameListener(frameListener)
                .initialSettings(settings)
                .connection(connection);

        if (debugLogging) {
            Http2FrameLogger frameLogger = new Http2FrameLogger(LogLevel.INFO,
                    NettyHttpClientRequestInitializer.class);
            builder.frameLogger(frameLogger);
        }

        HttpToHttp2ConnectionHandler connectionHandler = builder.build();

        return connectionHandler;
    }
}
