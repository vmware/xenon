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

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.net.ssl.SSLSession;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.AsciiString;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderUtil;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.AuthorizationContext;
import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.ServiceErrorResponse;
import com.vmware.dcp.common.ServiceHost;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.services.common.authn.AuthenticationConstants;

/**
 * Processes client requests on behalf of the HTTP listener and submits them to the service host or websocket client for
 * processing
 */
public class NettyHttpClientRequestHandler extends SimpleChannelInboundHandler<Object> {

    private final ServiceHost host;

    public NettyHttpClientRequestHandler(ServiceHost host) {
        this.host = host;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, Object msg) {
        try {
            Operation request = ctx.channel().attr(NettyChannelContext.OPERATION_KEY).get();
            if (!(msg instanceof FullHttpRequest)) {
                return;
            }
            // Start of request processing, initialize in-bound operation
            FullHttpRequest nettyRequest = (FullHttpRequest) msg;

            long expMicros = Utils.getNowMicrosUtc() + this.host.getOperationTimeoutMicros();
            try {
                URI messageUri = new URI(nettyRequest.uri());
                request = Operation.createGet(null);
                request.setAction(Action.valueOf(nettyRequest.method().toString()))
                        .setExpiration(expMicros);
                request.setUri(UriUtils.buildUri(this.host, messageUri.getPath(),
                        messageUri.getQuery()));
                ctx.channel().attr(NettyChannelContext.OPERATION_KEY).set(request);
            } catch (URISyntaxException e) {
                sendResponse(ctx, request);
                return;
            }

            if (nettyRequest.decoderResult().isFailure()) {
                request.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST);
                request.setBody(ServiceErrorResponse.create(nettyRequest.decoderResult()
                        .cause(),
                        request.getStatusCode()));
                sendResponse(ctx, request);
                return;
            }

            parseRequestHeaders(ctx, request, nettyRequest);

            decodeRequestBody(ctx, request, nettyRequest.content());

        } catch (Throwable e) {
            this.host.log(Level.SEVERE, "Uncaught exception: %s", Utils.toString(e));
        }
    }

    private void decodeRequestBody(ChannelHandlerContext ctx, Operation request, ByteBuf content) {
        if (!content.isReadable()) {
            // skip body decode, request had no body
            request.setContentLength(0);
            submitRequest(ctx, request);
            return;
        }

        request.nestCompletion((o, e) -> {
            if (e != null) {
                request.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST);
                request.setBody(ServiceErrorResponse.create(e, request.getStatusCode()));
                sendResponse(ctx, request);
                return;
            }
            submitRequest(ctx, request);
        });

        Utils.decodeBody(request, content.nioBuffer());
    }

    private void parseRequestHeaders(ChannelHandlerContext ctx, Operation request,
            HttpRequest nettyRequest) {
        HttpHeaders headers = nettyRequest.headers();

        String referer = headers.getAndRemoveAndConvert(HttpHeaderNames.REFERER);
        if (referer != null) {
            try {
                request.setReferer(new URI(referer));
            } catch (URISyntaxException e) {
                setRefererFromSocketContext(ctx, request);
            }
        } else {
            setRefererFromSocketContext(ctx, request);
        }

        if (headers.isEmpty()) {
            return;
        }

        request.setKeepAlive(HttpHeaderUtil.isKeepAlive(nettyRequest));
        if (HttpHeaderUtil.isContentLengthSet(nettyRequest)) {
            request.setContentLength(HttpHeaderUtil.getContentLength(nettyRequest));
        }

        request.setContextId(headers.getAndRemoveAndConvert(Operation.CONTEXT_ID_HEADER));

        String contentType = headers.getAndRemoveAndConvert(HttpHeaderNames.CONTENT_TYPE);
        if (contentType != null) {
            request.setContentType(contentType);
        }

        String cookie = headers.getAndRemoveAndConvert(HttpHeaderNames.COOKIE);
        if (cookie != null) {
            request.setCookies(CookieJar.decodeCookies(cookie));
        }

        for (Entry<String, String> h : headers.entriesConverted()) {
            String key = h.getKey();
            String value = h.getValue();
            request.addRequestHeader(key, value);
        }

        // Add peer Principal and CertificateChain to operation in case if client was successfully authenticated
        // by Netty using client certificate
        SslHandler sslHandler = (SslHandler) ctx.channel().pipeline()
                .get(NettyHttpServerInitializer.SSL_HANDLER);
        if (sslHandler != null) {
            try {
                if (sslHandler.engine().getWantClientAuth()
                        || sslHandler.engine().getNeedClientAuth()) {
                    SSLSession session = sslHandler.engine().getSession();
                    request.setPeerCertificates(session.getPeerPrincipal(),
                            session.getPeerCertificateChain());
                }
            } catch (Exception e) {
                this.host.log(Level.FINE, "Failed to get peer principal " + Utils.toString(e));
            }
        }
    }

    private void submitRequest(ChannelHandlerContext ctx, Operation request) {
        request.nestCompletion((o, e) -> {
            request.setBodyNoCloning(o.getBodyRaw());
            sendResponse(ctx, request);
        });

        Operation localOp = request;
        if (request.getRequestCallbackLocation() != null) {
            localOp = processRequestWithCallback(request);
        }

        this.host.handleRequest(null, localOp);
    }

    /**
     * Handles an operation that is split into the asynchronous HTTP pattern.
     * <p/>
     * 1) complete operation from remote peer immediately with ACCEPTED
     * <p/>
     * 2) Create a new local operation, cloned from the remote peer op, and set a completion that
     * will generate a PATCH to the remote callback location
     *
     * @param op
     * @return
     */
    private Operation processRequestWithCallback(Operation op) {
        final URI[] targetCallback = { null };
        try {
            targetCallback[0] = new URI(op.getRequestCallbackLocation());
        } catch (URISyntaxException e1) {
            op.fail(e1);
            return null;
        }

        Operation localOp = op.clone();

        // complete remote operation eagerly. We will PATCH the callback location with the
        // result when the local operation completes
        op.setStatusCode(Operation.STATUS_CODE_ACCEPTED).setBody(null).complete();

        localOp.setCompletion((o, e) -> {
            Operation patchForCompletion = Operation.createPatch(targetCallback[0])
                    .setReferer(o.getUri());
            int responseStatusCode = o.getStatusCode();
            if (e != null) {
                ServiceErrorResponse rsp = Utils.toServiceErrorResponse(e);
                rsp.statusCode = responseStatusCode;
                patchForCompletion.setBody(rsp);
            } else {
                if (!o.hasBody()) {
                    patchForCompletion.setBodyNoCloning(Operation.EMPTY_JSON_BODY);
                } else {
                    patchForCompletion.setBodyNoCloning(o.getBodyRaw());
                }
            }

            patchForCompletion.transferResponseHeadersToRequestHeadersFrom(o);
            patchForCompletion.addRequestHeader(
                    Operation.RESPONSE_CALLBACK_STATUS_HEADER,
                    Integer.toString(responseStatusCode));
            this.host.sendRequest(patchForCompletion);
        });

        return localOp;
    }

    private void sendResponse(ChannelHandlerContext ctx, Operation request) {
        try {
            writeResponseUnsafe(ctx, request);
        } catch (Throwable e1) {
            this.host.log(Level.SEVERE, "%s", Utils.toString(e1));
        }
    }

    private void writeResponseUnsafe(ChannelHandlerContext ctx, Operation request) {
        ByteBuf bodyBuffer = null;
        FullHttpResponse response;
        try {
            byte[] data = Utils.encodeBody(request);
            if (data != null) {
                bodyBuffer = Unpooled.wrappedBuffer(data);
            }
        } catch (Throwable e1) {
            // Note that this is a program logic error - some service isn't properly checking or setting Content-Type
            this.host.log(Level.SEVERE, "Error encoding body: %s", Utils.toString(e1));
            byte[] data;
            try {
                data = ("Error encoding body: " + e1.getMessage()).getBytes(Utils.CHARSET);
            } catch (UnsupportedEncodingException ueex) {
                this.exceptionCaught(ctx, ueex);
                return;
            }
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    Unpooled.wrappedBuffer(data));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, request.getContentType());
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
                    response.content().readableBytes());
            writeResponse(ctx, request, response);
            return;
        }

        if (bodyBuffer == null || request.getStatusCode() == Operation.STATUS_CODE_NOT_MODIFIED) {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.valueOf(request.getStatusCode()));
        } else {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.valueOf(request.getStatusCode()), bodyBuffer);
        }

        response.headers().set(HttpHeaderNames.CONTENT_TYPE,
                request.getContentType());
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
                response.content().readableBytes());

        // add any other custom headers associated with operation
        for (Entry<String, String> nameValue : request.getResponseHeaders().entrySet()) {
            response.headers().set(nameValue.getKey(), nameValue.getValue());
        }

        // Add Set-Cookie header to response if authorization context is marked as internal.
        AuthorizationContext authorizationContext = request.getAuthorizationContext();
        if (authorizationContext != null && authorizationContext.shouldPropagateToClient()) {
            StringBuilder buf = new StringBuilder()
                    .append(AuthenticationConstants.DCP_JWT_COOKIE)
                    .append('=')
                    .append(authorizationContext.getToken());

            // Add Path qualifier, cookie applies everywhere
            buf.append("; Path=/");
            // Add an Max-Age qualifier if an expiry is set in the Claims object
            if (authorizationContext.getClaims().getExpirationTime() != null) {
                buf.append("; Max-Age=");
                long maxAge = authorizationContext.getClaims().getExpirationTime() - Utils.getNowMicrosUtc();
                buf.append(maxAge > 0 ? TimeUnit.MICROSECONDS.toSeconds(maxAge) : 0);
            }
            response.headers().add(Operation.SET_COOKIE_HEADER, buf.toString());
        }

        writeResponse(ctx, request, response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Operation op = ctx.attr(NettyChannelContext.OPERATION_KEY).get();
        if (op != null) {
            this.host.log(Level.SEVERE, "Listener channel exception: %s, in progress op: %s",
                    cause.getMessage(), op.toString());
        }
        ctx.channel().attr(NettyChannelContext.OPERATION_KEY).remove();
        ctx.close();
    }

    private void setRefererFromSocketContext(ChannelHandlerContext ctx, Operation request) {
        try {
            InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
            String path = NettyHttpListener.UNKNOWN_CLIENT_REFERER_PATH;
            request.setReferer(UriUtils.buildUri(
                    remote.getHostString(),
                    remote.getPort(),
                    path,
                    null));
        } catch (Throwable e) {
            this.host.log(Level.SEVERE, "%s", Utils.toString(e));
        }
    }

    private void writeResponse(ChannelHandlerContext ctx, Operation request,
            FullHttpResponse response) {
        boolean isClose = !request.isKeepAlive() || response == null;
        Object rsp = Unpooled.EMPTY_BUFFER;
        if (response != null) {
            AsciiString v = isClose ? HttpHeaderValues.CLOSE : HttpHeaderValues.KEEP_ALIVE;
            response.headers().set(HttpHeaderNames.CONNECTION, v);
            rsp = response;
        }

        ctx.channel().attr(NettyChannelContext.OPERATION_KEY).remove();

        ChannelFuture future = ctx.writeAndFlush(rsp);
        if (isClose) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
