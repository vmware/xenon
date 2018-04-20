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

import java.util.function.BiConsumer;
import java.util.logging.Level;
import javax.net.ssl.SSLEngine;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslHandler;

import com.vmware.xenon.common.Utils;

/**
 * This sets up the channel pipeline for the Netty client.
 *
 * The default cause is that we set up a pipeline that handles HTTP/1.1, but if
 * forceHttp2 is true, we set up a pipeline that can be upgraded to HTTP/2.
 *
 */
public class NettyHttpClientRequestInitializer extends ChannelInitializer<SocketChannel> {

    private static class Http2NegotiationHandler extends ApplicationProtocolNegotiationHandler {

        private NettyHttpClientRequestInitializer initializer;
        private ChannelPromise settingsPromise;

        Http2NegotiationHandler(NettyHttpClientRequestInitializer initializer,
                ChannelPromise settingsPromise) {
            super("Fallback protocol");
            this.initializer = initializer;
            this.settingsPromise = settingsPromise;
        }

        @Override
        protected void configurePipeline(ChannelHandlerContext ctx, String protocol)
                throws Exception {
            try {
                if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
                    this.initializer.initializeHttp2Pipeline(ctx.pipeline(), this.settingsPromise);
                    return;
                }
                throw new IllegalStateException("Unexpected protocol: " + protocol);
            } catch (Exception ex) {
                log(Level.WARNING, "HTTP/2 pipeline initialization failed: %s",
                        Utils.toString(ex));
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

    public static final String ALPN_HANDLER = "alpn";
    public static final String SSL_HANDLER = "ssl";
    public static final String HTTP1_CODEC = "http1-codec";
    public static final String HTTP2_HANDLER = "http2-handler";
    public static final String UPGRADE_HANDLER = "upgrade-handler";
    public static final String UPGRADE_REQUEST = "upgrade-request";
    public static final String AGGREGATOR_HANDLER = "aggregator";
    public static final String XENON_HANDLER = "xenon";
    public static final String EVENT_LOGGER = "event-logger";
    public static final String EVENT_STREAM_HANDLER = "event-stream-handler";

    private final NettyChannelPool pool;
    private boolean isHttp2Only = false;
    private boolean debugLogging = false;
    private int requestPayloadSizeLimit;
    private BiConsumer<NettyChannelPool, Channel> onInitChannelConsumer;

    public NettyHttpClientRequestInitializer(
            NettyChannelPool nettyChannelPool,
            boolean isHttp2Only,
            int requestPayloadSizeLimit,
            BiConsumer<NettyChannelPool, Channel> onInitChannelConsumer) {
        this.pool = nettyChannelPool;
        this.isHttp2Only = isHttp2Only;
        this.requestPayloadSizeLimit = requestPayloadSizeLimit;
        this.onInitChannelConsumer = onInitChannelConsumer;
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
        ChannelPromise settingsPromise = ch.newPromise();
        ch.attr(NettyChannelContext.SETTINGS_PROMISE_KEY).set(settingsPromise);

        if (this.pool.getHttp2SslContext() != null) {
            if (!this.isHttp2Only) {
                throw new IllegalStateException("HTTP/2 must be enabled to set an SSL context");
            }
            p.addLast(SSL_HANDLER, this.pool.getHttp2SslContext().newHandler(ch.alloc()));
            p.addLast(ALPN_HANDLER, new Http2NegotiationHandler(this, settingsPromise));
            return;
        }

        if (this.pool.getSSLContext() != null) {
            if (this.isHttp2Only) {
                throw new IllegalArgumentException("HTTP/2 with SSL is not supported");
            }
            SSLEngine engine = this.pool.getSSLContext().createSSLEngine();
            engine.setUseClientMode(true);
            p.addLast(SSL_HANDLER, new SslHandler(engine));
        }

        HttpClientCodec http1_codec = new HttpClientCodec(
                NettyChannelContext.MAX_INITIAL_LINE_LENGTH,
                NettyChannelContext.MAX_HEADER_SIZE,
                NettyChannelContext.MAX_CHUNK_SIZE, false, false);

        // The HttpClientCodec combines the HttpRequestEncoder and the HttpResponseDecoder, and it
        // also provides a method for upgrading the protocol, which we use to support HTTP/2.
        p.addLast(HTTP1_CODEC, http1_codec);

        if (this.isHttp2Only) {
            try {
                NettyHttpToHttp2Handler connectionHandler = makeHttp2ConnectionHandler();
                Http2ClientUpgradeCodec upgradeCodec = new Http2ClientUpgradeCodec(
                        connectionHandler);
                HttpClientUpgradeHandler upgradeHandler = new HttpClientUpgradeHandler(
                        http1_codec,
                        upgradeCodec,
                        this.requestPayloadSizeLimit);

                p.addLast(UPGRADE_HANDLER, upgradeHandler);
                p.addLast(UPGRADE_REQUEST, new UpgradeRequestHandler());
                p.addLast("settings-handler", new Http2SettingsHandler(settingsPromise));
                p.addLast(EVENT_LOGGER, new NettyHttp2UserEventLogger(this.debugLogging));
            } catch (Exception ex) {
                Utils.log(NettyHttpClientRequestInitializer.class,
                        NettyHttpClientRequestInitializer.class.getSimpleName(),
                        Level.WARNING, "Channel Initializer exception: %s", ex);
                throw ex;
            }
        } else {
            p.addLast(EVENT_STREAM_HANDLER, new NettyHttpEventStreamHandler());
            // The HttpObjectAggregator is not needed for HTTP/2. For HTTP/1.1 it
            // aggregates the HttpMessage and HttpContent into the FullHttpResponse
            p.addLast(AGGREGATOR_HANDLER,
                    new HttpObjectAggregator(this.requestPayloadSizeLimit));
        }
        p.addLast(XENON_HANDLER, new NettyHttpServerResponseHandler(this.pool));

        // allow user callback to modify the channel/pipeline
        if (this.onInitChannelConsumer != null) {
            this.onInitChannelConsumer.accept(this.pool, ch);
        }
    }

    public void initializeHttp2Pipeline(ChannelPipeline p, ChannelPromise settingsPromise) {
        p.addLast(HTTP2_HANDLER, makeHttp2ConnectionHandler());
        p.addLast("settings-handler", new Http2SettingsHandler(settingsPromise));
        p.addLast(EVENT_LOGGER, new NettyHttp2UserEventLogger(this.debugLogging));
        p.addLast(XENON_HANDLER, new NettyHttpServerResponseHandler(this.pool));
    }

    /**
     * This special handler will send a GET to the HTTP/2 server. This will trigger
     * the HTTP/2 settings negotiation, which needs to be done before we send user data.
     * Once we've triggered it, we remove it from the pipeline.
     */
    private static final class UpgradeRequestHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            DefaultFullHttpRequest upgradeRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                    HttpMethod.GET, "/");
            context.writeAndFlush(upgradeRequest);
            super.channelActive(context);
            context.pipeline().remove(this);
        }
    }

    /**
     * For HTTP/2 we don't have anything as simple as the HttpClientCodec (at least, not in Netty
     * 5.0alpha 2), so we create the equivalent here.
     *
     * @return
     */
    private NettyHttpToHttp2Handler makeHttp2ConnectionHandler() {
        // DefaultHttp2Connection is for client or server. False means "client".
        Http2Connection connection = new DefaultHttp2Connection(false);
        InboundHttp2ToHttpAdapter inboundAdapter = new InboundHttp2ToHttpAdapterBuilder(connection)
                .maxContentLength(this.requestPayloadSizeLimit)
                .propagateSettings(true)
                .build();
        DelegatingDecompressorFrameListener frameListener = new DelegatingDecompressorFrameListener(
                connection, inboundAdapter);

        Http2Settings settings = new Http2Settings();
        settings.initialWindowSize(NettyChannelContext.INITIAL_HTTP2_WINDOW_SIZE);

        NettyHttpToHttp2HandlerBuilder builder = new NettyHttpToHttp2HandlerBuilder()
                .connection(connection)
                .frameListener(frameListener)
                .initialSettings(settings);
        if (this.debugLogging) {
            Http2FrameLogger frameLogger = new Http2FrameLogger(LogLevel.INFO,
                    NettyHttpClientRequestInitializer.class);
            builder.frameLogger(frameLogger);
        }
        NettyHttpToHttp2Handler connectionHandler = builder.build();

        return connectionHandler;
    }

    /**
     * This handler does just one thing: it informs us (via a promise) that we've received
     * an HTTP/2 settings frame. We do this because when we connect to the HTTP/2 server,
     * we have to wait until the settings have been negotiated before we send data.
     * NettyChannelPool uses this promise.
     *
     */
    private static class Http2SettingsHandler extends SimpleChannelInboundHandler<Http2Settings> {
        private ChannelPromise promise;

        public Http2SettingsHandler(ChannelPromise promise) {
            this.promise = promise;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Http2Settings msg)
                throws Exception {
            ctx.pipeline().remove(this);
            this.promise.setSuccess();
        }
    }

}
