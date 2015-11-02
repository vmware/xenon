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

import java.util.EnumSet;
import java.util.logging.Logger;
import javax.net.ssl.SSLEngine;

import static io.netty.util.internal.logging.InternalLogLevel.INFO;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;
import io.netty.handler.codec.http.HttpMethod;

import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2ClientUpgradeCodec;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.SocketContext;
import com.vmware.dcp.common.ServiceErrorResponse;
import com.vmware.dcp.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.dcp.common.Utils;

public class NettyHttpClientRequestInitializer extends ChannelInitializer<SocketChannel> {
    private static final Http2FrameLogger logger =
            new Http2FrameLogger(INFO, InternalLoggerFactory.getInstance(NettyHttpClientRequestInitializer.class));

    public static final String AGGREGATOR_HANDLER = "aggregator";
    public static final String DCP_HANDLER = "dcp";
    public static final String DECODER_HANDLER = "decoder";
    public static final String ENCODER_HANDLER = "encoder";
    public static final String SSL_HANDLER = "ssl";

    private final NettyChannelPool pool;
    private HttpToHttp2ConnectionHandler connectionHandler;
    private boolean tryUpgrade;
    private NettyHttp2ClientSettingsHandler settingsHandler;

    public NettyHttpClientRequestInitializer(NettyChannelPool nettyChannelPool,
                                             boolean tryUpgrade) {
        this.pool = nettyChannelPool;
        this.tryUpgrade = tryUpgrade;
    }

    private final class UpgradeRequestHandler extends ChannelHandlerAdapter {
        public UpgradeRequestHandler() {
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            DefaultFullHttpRequest upgradeRequest =
                    new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            ctx.writeAndFlush(upgradeRequest);

            super.channelActive(ctx);

            // Done with this handler, remove it from the pipeline.
            ChannelPipeline p = ctx.pipeline();
            p.remove(this);

            p.addLast(NettyHttpClientRequestInitializer.this.settingsHandler);
            p.addLast(new NettyHttpServerResponseHandler(NettyHttpClientRequestInitializer.this.pool));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause != null) {
                Logger.getAnonymousLogger().warning(Utils.toString(cause));
            }

            Operation request = ctx.channel().attr(NettyChannelContext.OPERATION_KEY).get();

            if (request == null) {
                Logger.getAnonymousLogger().info("no request associated with channel");
                return;
            }

            // I/O exception this code recommends retry since it never made it to the remote end
            request.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST);
            request.setBody(ServiceErrorResponse.create(cause, request.getStatusCode(),
                    EnumSet.of(ErrorDetail.SHOULD_RETRY)));
            request.fail(cause);

        }
    }

    public NettyHttp2ClientSettingsHandler settingsHandler() {
        return this.settingsHandler;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        ch.config().setAllocator(NettyChannelContext.ALLOCATOR);
        ch.config().setSendBufferSize(NettyChannelContext.BUFFER_SIZE);
        ch.config().setReceiveBufferSize(NettyChannelContext.BUFFER_SIZE);

        if (this.tryUpgrade) {
            final Http2Connection connection = new DefaultHttp2Connection(false);
            this.connectionHandler = new HttpToHttp2ConnectionHandler(connection,
                    frameReader(),
                    frameWriter(),
                    new DelegatingDecompressorFrameListener(connection,
                            new InboundHttp2ToHttpAdapter.Builder(connection)
                                .maxContentLength(SocketContext.MAX_REQUEST_SIZE)
                                    .propagateSettings(true)
                                    .build()));

            this.settingsHandler = new NettyHttp2ClientSettingsHandler(ch.newPromise());

            if (this.pool.getNettySslContext() != null) {
                //TLS HTTP/2 upgrade pipeline
                p.addLast("ssl", this.pool.getNettySslContext().newHandler(ch.alloc()));
                p.addLast("Http2Handler", this.connectionHandler);
                p.addLast("encoder", new HttpRequestEncoder());
                p.addLast("decoder", new HttpResponseDecoder());
                p.addLast("aggregator", new HttpObjectAggregator(SocketContext.MAX_REQUEST_SIZE));
                p.addLast(new NettyHttpServerResponseHandler(this.pool));
            } else {
                //Clear text HTTP/2 upgrade pipeline
                HttpClientCodec sourceCodec = new HttpClientCodec();
                Http2ClientUpgradeCodec upgradeCodec = new Http2ClientUpgradeCodec(this.connectionHandler);
                HttpClientUpgradeHandler upgradeHandler = new HttpClientUpgradeHandler(sourceCodec,
                        upgradeCodec, SocketContext.MAX_REQUEST_SIZE);

                p.addLast("Http2SourceCodec", sourceCodec);
                p.addLast("Http2UpgradeHandler", upgradeHandler);
                p.addLast("Http2UpgradeRequestHandler",
                        new UpgradeRequestHandler());
                p.addLast("Logger", new UserEventLogger());
            }
        } else {
            //Create an HTTP/1.1 pipeline
            if (this.pool.getSSLContext() != null) {
                SSLEngine engine = this.pool.getSSLContext().createSSLEngine();
                engine.setUseClientMode(true);
                p.addLast("ssl", new SslHandler(engine));
            }
            if (this.pool.getSSLContext() != null) {
                SSLEngine engine = this.pool.getSSLContext().createSSLEngine();
                engine.setUseClientMode(true);
                p.addLast(SSL_HANDLER, new SslHandler(engine));
            }
            p.addLast(ENCODER_HANDLER, new HttpRequestEncoder());
            p.addLast(DECODER_HANDLER, new HttpResponseDecoder());
            p.addLast(AGGREGATOR_HANDLER, new HttpObjectAggregator(SocketContext.MAX_REQUEST_SIZE));
            p.addLast(DCP_HANDLER, new NettyHttpServerResponseHandler(this.pool));
        }
    }

    private static class UserEventLogger extends ChannelHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            System.out.println("User Event Triggered: " + evt);
            super.userEventTriggered(ctx, evt);
        }
    }

    private static Http2FrameReader frameReader() {
        return new Http2InboundFrameLogger(new DefaultHttp2FrameReader(), logger);
    }

    private static Http2FrameWriter frameWriter() {
        return new Http2OutboundFrameLogger(new DefaultHttp2FrameWriter(), logger);
    }
}