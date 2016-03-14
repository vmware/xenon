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
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AsciiString;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;

/**
 * Processes client requests on behalf of the HTTP listener and submits them to the service host or websocket client for
 * processing
 */
public class NettyHttpClientRequestHandler extends SimpleChannelInboundHandler<Object> {

    private static final String ERROR_MSG_DECODING_FAILURE = "Failure decoding HTTP request";

    private final ServiceHost host;

    private final SslHandler sslHandler;

    public NettyHttpClientRequestHandler(ServiceHost host, SslHandler sslHandler) {
        this.host = host;
        this.sslHandler = sslHandler;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            return true;
        }
        return false;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        Operation request = null;
        Integer streamId = null;
        try {
            // Start of request processing, initialize in-bound operation
            FullHttpRequest nettyRequest = (FullHttpRequest) msg;
            long expMicros = Utils.getNowMicrosUtc() + this.host.getOperationTimeoutMicros();
            URI targetUri = new URI(nettyRequest.uri()).normalize();
            request = Operation.createGet(null);
            request.setAction(Action.valueOf(nettyRequest.method().toString()))
                    .setExpiration(expMicros);

            String query = targetUri.getQuery();
            if (query != null && !query.isEmpty()) {
                query = QueryStringDecoder.decodeComponent(targetUri.getQuery());
            }

            URI uri = new URI(UriUtils.HTTP_SCHEME, null, ServiceHost.LOCAL_HOST,
                    this.host.getPort(), targetUri.getPath(), query, null);
            request.setUri(uri);

            // The streamId will be null for HTTP/1.1 connections, and valid for HTTP/2 connections
            streamId = nettyRequest.headers().getInt(
                    HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
            if (streamId == null) {
                ctx.channel().attr(NettyChannelContext.OPERATION_KEY).set(request);
            } else {
                InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
                int port = localAddress.getPort();
            }

            if (nettyRequest.decoderResult().isFailure()) {
                request.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST).setKeepAlive(false);
                request.setBody(ServiceErrorResponse.create(
                        new IllegalArgumentException(ERROR_MSG_DECODING_FAILURE),
                        request.getStatusCode()));
                sendResponse(ctx, request, streamId);
                return;
            }

            parseRequestHeaders(ctx, request, nettyRequest);
            decodeRequestBody(ctx, request, nettyRequest.content(), streamId);
        } catch (Throwable e) {
            this.host.log(Level.SEVERE, "Uncaught exception: %s", Utils.toString(e));
            if (request == null) {
                request = Operation.createGet(this.host.getUri());
            }
            int sc = Operation.STATUS_CODE_BAD_REQUEST;
            if (e instanceof URISyntaxException) {
                request.setUri(this.host.getUri());
            }
            request.setKeepAlive(false).setStatusCode(sc)
                    .setBodyNoCloning(ServiceErrorResponse.create(e, sc));
            sendResponse(ctx, request, streamId);
        }
    }

    private void decodeRequestBody(ChannelHandlerContext ctx, Operation request, ByteBuf content, Integer streamId) {
        if (!content.isReadable()) {
            // skip body decode, request had no body
            request.setContentLength(0);
            submitRequest(ctx, request, streamId);
            return;
        }

        request.nestCompletion((o, e) -> {
            if (e != null) {
                request.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST);
                request.setBody(ServiceErrorResponse.create(e, request.getStatusCode()));
                sendResponse(ctx, request, streamId);
                return;
            }
            submitRequest(ctx, request, streamId);
        });

        Utils.decodeBody(request, content.nioBuffer());
    }

    private void parseRequestHeaders(ChannelHandlerContext ctx, Operation request,
            HttpRequest nettyRequest) {
        HttpHeaders headers = nettyRequest.headers();

        String referer = getAndRemove(headers, HttpHeaderNames.REFERER);
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

        request.setKeepAlive(HttpUtil.isKeepAlive(nettyRequest));
        if (HttpUtil.isContentLengthSet(nettyRequest)) {
            request.setContentLength(HttpUtil.getContentLength(nettyRequest));
        }

        request.setContextId(getAndRemove(headers, Operation.CONTEXT_ID_HEADER));

        String contentType = getAndRemove(headers, HttpHeaderNames.CONTENT_TYPE);
        if (contentType != null) {
            request.setContentType(contentType);
        }

        String cookie = getAndRemove(headers, HttpHeaderNames.COOKIE);
        if (cookie != null) {
            request.setCookies(CookieJar.decodeCookies(cookie));
        }

        for (Entry<String, String> h : headers) {
            String key = h.getKey();
            String value = h.getValue();
            request.addRequestHeader(key, value);
        }

        if (this.sslHandler == null) {
            return;
        }
        try {
            if (this.sslHandler.engine().getWantClientAuth()
                    || this.sslHandler.engine().getNeedClientAuth()) {
                SSLSession session = this.sslHandler.engine().getSession();
                request.setPeerCertificates(session.getPeerPrincipal(),
                        session.getPeerCertificateChain());
            }
        } catch (Exception e) {
            this.host.log(Level.FINE, "Failed to get peer principal " + Utils.toString(e));
        }
    }

    private String getAndRemove(HttpHeaders headers, AsciiString headerName) {
        String headerValue = headers.get(headerName.toString());
        headers.remove(headerName);
        return headerValue;
    }

    private String getAndRemove(HttpHeaders headers, String headerName) {
        String headerValue = headers.get(headerName);
        headers.remove(headerName);
        return headerValue;
    }

    private void submitRequest(ChannelHandlerContext ctx, Operation request, Integer streamId) {
        request.nestCompletion((o, e) -> {
            request.setBodyNoCloning(o.getBodyRaw());
            sendResponse(ctx, request, streamId);
        });

        request.setCloningDisabled(true);
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

    private void sendResponse(ChannelHandlerContext ctx, Operation request, Integer streamId) {
        try {
            writeResponseUnsafe(ctx, request, streamId);
        } catch (Throwable e1) {
            this.host.log(Level.SEVERE, "%s", Utils.toString(e1));
        }
    }

    private void writeResponseUnsafe(ChannelHandlerContext ctx, Operation request, Integer streamId) {
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
                    Unpooled.wrappedBuffer(data), false, false);
            if (streamId != null) {
                response.headers().setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(),
                        streamId);
            }
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, Operation.MEDIA_TYPE_TEXT_HTML);
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
                    response.content().readableBytes());
            writeResponse(ctx, request, response);
            return;
        }

        if (bodyBuffer == null || request.getStatusCode() == Operation.STATUS_CODE_NOT_MODIFIED) {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.valueOf(request.getStatusCode()), false, false);
        } else {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.valueOf(request.getStatusCode()), bodyBuffer, false, false);
        }

        if (streamId != null) {
            // This is the stream ID from the incoming request: we need to use it for our
            // response so the client knows this is the response. If we don't set the stream
            // ID, Netty assigns a new, unused stream, which would be bad.
            response.headers().setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(),
                    streamId);
        }
        response.headers().set(HttpHeaderNames.CONTENT_TYPE,
                request.getContentType());
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
                response.content().readableBytes());

        // add any other custom headers associated with operation
        for (Entry<String, String> nameValue : request.getResponseHeaders().entrySet()) {
            response.headers().set(nameValue.getKey(), nameValue.getValue());
        }

        // Add auth token to response if authorization context
        AuthorizationContext authorizationContext = request.getAuthorizationContext();
        if (authorizationContext != null && authorizationContext.shouldPropagateToClient()) {
            String token = authorizationContext.getToken();

            // The x-xenon-auth-token header is our preferred style
            response.headers().add(Operation.REQUEST_AUTH_TOKEN_HEADER, token);

            // Client can also use the cookie if they prefer
            StringBuilder buf = new StringBuilder()
                    .append(AuthenticationConstants.XENON_JWT_COOKIE)
                    .append('=')
                    .append(token);

            // Add Path qualifier, cookie applies everywhere
            buf.append("; Path=/");
            // Add an Max-Age qualifier if an expiration is set in the Claims object
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
            this.host.log(Level.SEVERE,
                    "HTTP/1.1 listener channel exception: %s, in progress op: %s",
                    cause.getMessage(), op.toString());
        } else {
            // This case may be hit for HTTP/2 connections, which do not have
            // a single set of operations associated with them.
            this.host.log(Level.SEVERE, "Listener channel exception: %s",
                    cause.getMessage());
        }
        ctx.channel().attr(NettyChannelContext.OPERATION_KEY).remove();
        ctx.close();
    }

    private void setRefererFromSocketContext(ChannelHandlerContext ctx, Operation request) {
        try {
            InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
            String path = NettyHttpListener.UNKNOWN_CLIENT_REFERER_PATH;
            request.setReferer(UriUtils.buildUri(
                    this.sslHandler != null ? "https" : "http",
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
