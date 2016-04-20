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

package com.vmware.xenon.common.test.websockets;

import java.net.URI;
import java.util.Collections;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.annotations.JSFunction;
import org.mozilla.javascript.annotations.JSGetter;
import org.mozilla.javascript.annotations.JSSetter;

import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.http.netty.CookieJar;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;

/**
 * Lightweight partial implementation of JS Websocket API
 */
public class JsWebSocket extends ScriptableObject {
    private static final long serialVersionUID = 0L;
    public static final String DATA = "data";
    public static final String WS_SCHEME = "ws";
    public static final String WSS_SCHEME = "wss";
    public static final String CLASS_NAME = "WebSocket";
    public static final String TYPE = "type";
    public static final String ERROR = "error";

    private EventLoopGroup group;
    private Channel channel;
    private Function onopen;
    private Function onerror;
    private Function onmessage;
    private Function onclose;

    private class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private volatile ChannelPromise handshakeFuture;

        public WebSocketClientHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (!this.handshaker.isHandshakeComplete()) {
                try {
                    this.handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                    this.handshakeFuture.setSuccess();
                } catch (Exception e) {
                    if (JsWebSocket.this.onerror != null) {
                        NativeObject event = new NativeObject();
                        event.defineProperty(TYPE, ERROR, 0);
                        JsExecutor.executeSynchronously(() ->
                                JsWebSocket.this.onerror.call(Context.getCurrentContext(),
                                        getParentScope(),
                                        JsWebSocket.this,
                                        new Object[] { event }));
                    }
                }
                return;
            }
            if (msg instanceof WebSocketFrame) {
                if (msg instanceof PingWebSocketFrame) {
                    PingWebSocketFrame frame = (PingWebSocketFrame) msg;
                    ctx.channel().writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
                    return;
                }
                if (msg instanceof TextWebSocketFrame) {
                    TextWebSocketFrame frame = (TextWebSocketFrame) msg;
                    if (JsWebSocket.this.onmessage != null) {
                        NativeObject event = new NativeObject();
                        event.defineProperty(DATA, frame.text(), 0);
                        JsExecutor.executeSynchronously(() ->
                                JsWebSocket.this.onmessage.call(Context.getCurrentContext(),
                                        getParentScope(),
                                        JsWebSocket.this,
                                        new Object[] { event }));
                    }
                }
                if (msg instanceof BinaryWebSocketFrame) {
                    BinaryWebSocketFrame frame = (BinaryWebSocketFrame) msg;
                    if (JsWebSocket.this.onmessage != null) {
                        byte[] data = new byte[frame.content().readableBytes()];
                        frame.content().readBytes(data);
                        JsExecutor.executeSynchronously(() ->
                                JsWebSocket.this.onmessage.call(Context.getCurrentContext(),
                                        getParentScope(),
                                        JsWebSocket.this,
                                        new Object[] { data }));
                    }
                }
                if (msg instanceof CloseWebSocketFrame) {
                    ctx.channel().close();
                }
            }
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            super.handlerAdded(ctx);
            this.handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            this.handshaker.handshake(ctx.channel());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            super.exceptionCaught(ctx, cause);
        }
    }

    /**
     * Public constructor used by Rhino to create a prototype.
     */
    public JsWebSocket() {
    }

    /**
     * Standard constructor WebSocket(uri) available in JavaScript API
     *
     * @param endpointUri Websocket endpoint URI
     */
    public JsWebSocket(String endpointUri) throws Exception {
        URI uri = new URI(endpointUri);
        String scheme = uri.getScheme() == null ? WS_SCHEME : uri.getScheme();
        final String host = uri.getHost() == null ? ServiceHost.LOCAL_HOST : uri.getHost();
        final int port;
        if (uri.getPort() == -1) {
            if (WS_SCHEME.equalsIgnoreCase(scheme)) {
                port = 80;
            } else if (WSS_SCHEME.equalsIgnoreCase(scheme)) {
                port = 443;
            } else {
                port = -1;
            }
        } else {
            port = uri.getPort();
        }

        if (!WS_SCHEME.equalsIgnoreCase(scheme) && !WSS_SCHEME.equalsIgnoreCase(scheme)) {
            System.err.println("Only WS(S) is supported.");
            return;
        }

        final boolean ssl = WSS_SCHEME.equalsIgnoreCase(scheme);
        final SslContext sslCtx;
        if (ssl) {
            sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
        } else {
            sslCtx = null;
        }

        this.group = new NioEventLoopGroup();

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        if (OperationContext.getAuthorizationContext() != null
                && OperationContext.getAuthorizationContext().getToken() != null) {
            headers.add(HttpHeaderNames.COOKIE, CookieJar.encodeCookies(Collections.singletonMap(
                    AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE,
                    OperationContext.getAuthorizationContext().getToken())));
        }
        final WebSocketClientHandler handler = new WebSocketClientHandler(
                WebSocketClientHandshakerFactory.newHandshaker(
                        uri, WebSocketVersion.V13, null, false, headers));
        Bootstrap b = new Bootstrap();
        b.group(this.group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (sslCtx != null) {
                            p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                        }
                        p.addLast(
                                new HttpClientCodec(),
                                new HttpObjectAggregator(8192),
                                handler);
                    }
                });

        this.channel = b.connect(uri.getHost(), port).sync().channel();
        handler.handshakeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                try {
                    JsExecutor.executeSynchronously(() -> {
                        if (future.isSuccess()) {
                            if (JsWebSocket.this.onopen != null) {
                                JsWebSocket.this.onopen.call(Context.getCurrentContext(),
                                        getParentScope(),
                                        JsWebSocket.this, new
                                        Object[] { null });
                            }
                        } else {
                            throw new RuntimeException(future.cause());
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Sends text websocket frame.
     *
     * @param data Frame content.
     */
    @JSFunction
    public void send(String data) {
        this.channel.writeAndFlush(new TextWebSocketFrame(data));
    }

    /**
     * Closes a websocket.
     */
    @JSFunction
    public void close() {
        this.group.shutdownGracefully();
    }

    @JSGetter
    public Function getOnmessage() {
        return this.onmessage;
    }

    @JSSetter
    public void setOnmessage(Function onmessage) {
        this.onmessage = onmessage;
    }

    @JSGetter
    public Function getOnclose() {
        return this.onclose;
    }

    @JSSetter
    public void setOnclose(Function onclose) {
        this.onclose = onclose;
    }

    @JSGetter
    public Function getOnopen() {
        return this.onopen;
    }

    @JSSetter
    public void setOnopen(Function onopen) {
        this.onopen = onopen;
    }

    @JSGetter
    public Function getOnerror() {
        return this.onerror;
    }

    @JSSetter
    public void setOnerror(Function onerror) {
        this.onerror = onerror;
    }

    @Override
    public String getClassName() {
        return CLASS_NAME;
    }

}
