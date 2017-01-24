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

import io.netty.channel.ChannelHandlerContext;
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
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AsciiString;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.SslClientAuthMode;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class NettyHttpServerInitializer extends ChannelInitializer<SocketChannel> {

    private static class Http2NegotiationHandler extends ApplicationProtocolNegotiationHandler {

        private NettyHttpServerInitializer initializer;
        private SslHandler sslHandler;

        Http2NegotiationHandler(NettyHttpServerInitializer initializer, SslHandler sslHandler) {
            super(ApplicationProtocolNames.HTTP_1_1);
            this.initializer = initializer;
            this.sslHandler = sslHandler;
        }

        @Override
        protected void configurePipeline(ChannelHandlerContext ctx, String protocol)
                throws Exception {
            try {
                if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
                    this.initializer.initializeHttp2Pipeline(ctx.pipeline(), this.sslHandler);
                    return;
                }
                if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
                    this.initializer.initializeHttpPipeline(ctx.pipeline(), this.sslHandler);
                    return;
                }
                throw new IllegalStateException("Unexpected protocol: " + protocol);
            } catch (Exception ex) {
                log(Level.WARNING, "Pipeline initialization failed: %s", Utils.toString(ex));
                ctx.close();
            }
        }

        @Override
        protected void handshakeFailure(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            log(Level.WARNING, "TLS handshake failed: %s", Utils.toString(cause));
            ctx.close();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            log(Level.WARNING, "ALPN protocol negotiation failed: %s", Utils.toString(cause));
            ctx.close();
        }

        private void log(Level level, String fmt, Object... args) {
            Utils.log(Http2NegotiationHandler.class, Http2NegotiationHandler.class.getSimpleName(),
                    level, fmt, args);
        }
    }

    public static final String AGGREGATOR_HANDLER = "aggregator";
    public static final String ALPN_HANDLER = "alpn-handler";
    public static final String HTTP_REQUEST_HANDLER = "http-request-handler";
    public static final String WEBSOCKET_HANDLER = "websocket-request-handler";
    public static final String HTTP1_CODEC = "http1-codec";
    public static final String HTTP2_HANDLER = "http2-handler";
    public static final String HTTP2_UPGRADE_HANDLER = "http2-upgrade-handler";
    public static final String SSL_HANDLER = "ssl";

    private final SslContext sslContext;
    private ServiceHost host;
    private NettyHttpListener listener;
    private int responsePayloadSizeLimit;
    private static final boolean debugLogging = false;

    public NettyHttpServerInitializer(NettyHttpListener listener, ServiceHost host,
            SslContext sslContext, int responsePayloadSizeLimit) {
        this.sslContext = sslContext;
        this.host = host;
        this.listener = listener;
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

            if (NettyChannelContext.isALPNEnabled()) {
                p.addLast(ALPN_HANDLER, new Http2NegotiationHandler(this, sslHandler));
                return;
            }
        }

        initializeHttpPipeline(p, sslHandler);
    }

    private void initializeHttp2Pipeline(ChannelPipeline pipeline, SslHandler sslHandler) {
        pipeline.addLast(HTTP2_HANDLER, makeHttp2ConnectionHandler());
        initializeCommon(pipeline, sslHandler);
    }

    private void initializeHttpPipeline(ChannelPipeline p, SslHandler sslHandler) {
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
            // Configure for plaintext upgrade to HTTP/2
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
        initializeCommon(p, sslHandler);
    }

    private void initializeCommon(ChannelPipeline p, SslHandler sslHandler) {
        p.addLast(WEBSOCKET_HANDLER, new NettyWebSocketRequestHandler(this.host,
                ServiceUriPaths.CORE_WEB_SOCKET_ENDPOINT,
                ServiceUriPaths.WEB_SOCKET_SERVICE_PREFIX));
        p.addLast(HTTP_REQUEST_HANDLER,
                new NettyHttpClientRequestHandler(this.host, this.listener, sslHandler,
                        this.responsePayloadSizeLimit));
    }

    /**
     * For HTTP/2 we don't have anything as simple as the HttpServerCodec (at least, not in Netty
     * 5.0alpha 2), so we create the equivalent.
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
