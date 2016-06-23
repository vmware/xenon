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
import com.vmware.xenon.common.Operation.OperationOption;
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

    private int responsePayloadSizeLimit;

    public NettyHttpClientRequestHandler(ServiceHost host, SslHandler sslHandler,
            int responsePayloadSizeLimit) {
        this.host = host;
        this.sslHandler = sslHandler;
        this.responsePayloadSizeLimit = responsePayloadSizeLimit;
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

            // @see OperationOption.SEND_WITH_CALLBACK
            String callbackLocation = getAndRemove(nettyRequest.headers(),
                    Operation.REQUEST_CALLBACK_LOCATION_HEADER);
            URI callbackUri = null;

            if (callbackLocation == null) {
                // The streamId will be null for HTTP/1.1 connections, and valid for HTTP/2 connections
                streamId = nettyRequest.headers().getInt(
                        HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
            } else {
                request.setReferer(callbackLocation);
                callbackUri = new URI(callbackLocation);
            }

            if (streamId == null) {
                ctx.channel().attr(NettyChannelContext.OPERATION_KEY).set(request);
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

            if (callbackLocation != null) {
                Operation localOp = request.clone();
                // complete remote operation eagerly. We will PATCH the callback location with the
                // result when the local operation completes
                request.setStatusCode(Operation.STATUS_CODE_ACCEPTED).setBody(null);
                sendResponse(ctx, request, null);
                request = localOp;
            }

            decodeRequestBody(ctx, request, nettyRequest.content(), streamId, callbackUri);
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

    private void decodeRequestBody(ChannelHandlerContext ctx, Operation request,
            ByteBuf content, Integer streamId, URI callbackUri) {
        if (!content.isReadable()) {
            // skip body decode, request had no body
            request.setContentLength(0);
            submitRequest(ctx, request, streamId, callbackUri);
            return;
        }

        request.nestCompletion((o, e) -> {
            if (e != null) {
                request.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST);
                request.setBody(ServiceErrorResponse.create(e, request.getStatusCode()));
                sendResponse(ctx, request, streamId);
                return;
            }

            submitRequest(ctx, request, streamId, callbackUri);
        });

        Utils.decodeBody(request, content.nioBuffer());

    }

    private void parseRequestHeaders(ChannelHandlerContext ctx, Operation request,
            HttpRequest nettyRequest) {

        HttpHeaders headers = nettyRequest.headers();
        boolean hasHeaders = !headers.isEmpty();

        String referer = getAndRemove(headers, Operation.REFERER_HEADER);
        if (referer != null) {
            request.setReferer(referer);
        }

        if (!hasHeaders) {
            return;
        }

        request.setKeepAlive(HttpUtil.isKeepAlive(nettyRequest));
        if (HttpUtil.isContentLengthSet(nettyRequest)) {
            request.setContentLength(HttpUtil.getContentLength(nettyRequest));
            getAndRemove(headers, Operation.CONTENT_LENGTH_HEADER);
        }

        String pragma = getAndRemove(headers, Operation.PRAGMA_HEADER);
        if (Operation.PRAGMA_DIRECTIVE_REPLICATED.equals(pragma)) {
            // replication requests will have a single PRAGMA directive. Set the right
            // options and remove the header to avoid further allocations
            request.setFromReplication(true).setTargetReplicated(true);
        } else if (pragma != null) {
            request.addRequestHeader(Operation.PRAGMA_HEADER, pragma);
        }

        if (request.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED)) {
            // synchronization requests will have additional directives, so check again here
            // if the request is replicated
            request.setFromReplication(true).setTargetReplicated(true);
        }

        request.setContextId(getAndRemove(headers, Operation.CONTEXT_ID_HEADER));

        request.setTransactionId(getAndRemove(headers, Operation.TRANSACTION_ID_HEADER));

        String contentType = getAndRemove(headers, Operation.CONTENT_TYPE_HEADER);
        if (contentType != null) {
            request.setContentType(contentType);
        }

        String cookie = getAndRemove(headers, Operation.COOKIE_HEADER);
        if (cookie != null) {
            request.setCookies(CookieJar.decodeCookies(cookie));
        }

        String host = getAndRemove(headers, Operation.HOST_HEADER);

        for (Entry<String, String> h : headers) {
            String key = h.getKey();
            String value = h.getValue();
            if (Operation.STREAM_ID_HEADER.equals(key)) {
                continue;
            }
            if (Operation.HTTP2_SCHEME_HEADER.equals(key)) {
                continue;
            }

            request.addRequestHeader(key, value);
        }

        if (host != null) {
            request.addRequestHeader(Operation.HOST_HEADER, host);
        }

        if (request.getRequestHeader(Operation.RESPONSE_CALLBACK_STATUS_HEADER) != null) {
            request.setReferer(request.getUri());
        }

        if (!request.hasReferer() && request.isFromReplication()) {
            // we assume referrer is the same service, but from the remote node. Do not
            // bother with rewriting the URI with the remote host, at avoid allocations
            request.setReferer(request.getUri());
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
            this.host.log(Level.WARNING, "Failed to get peer principal " + Utils.toString(e));
        }
    }

    private String getAndRemove(HttpHeaders headers, String headerName) {
        String headerValue = headers.get(headerName);
        headers.remove(headerName);
        return headerValue;
    }

    private void submitRequest(ChannelHandlerContext ctx, Operation request,
            Integer streamId, URI callbackLocation) {
        request.nestCompletion((o, e) -> {
            request.setBodyNoCloning(o.getBodyRaw());
            sendResponse(ctx, request, streamId);
        });

        request.toggleOption(OperationOption.CLONING_DISABLED, true);

        if (!request.hasReferer()) {
            setRefererFromSocketContext(ctx, request);
        }

        Operation localOp = request;
        if (callbackLocation != null) {
            localOp = processRequestWithCallback(request, callbackLocation);
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
     */
    private Operation processRequestWithCallback(Operation localOp, URI callbackLocation) {
        localOp.setCompletion((o, e) -> {
            Operation patchForCompletion = Operation.createPatch(callbackLocation)
                    .setReferer(o.getUri());
            int responseStatusCode = o.getStatusCode();
            if (!o.hasBody()) {
                patchForCompletion.setBodyNoCloning(Operation.EMPTY_JSON_BODY);
            } else {
                patchForCompletion.setBodyNoCloning(o.getBodyRaw());
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

            // if some service returns a response that is greater than the maximum allowed size,
            // we return an INTERNAL_SERVER_ERROR.
            if (request.getContentLength() > this.responsePayloadSizeLimit) {
                String errorMessage = "Content-Length " + request.getContentLength()
                        + " is greater than max size allowed " + this.responsePayloadSizeLimit;
                this.host.log(Level.SEVERE, errorMessage);
                writeInternalServerError(ctx, request, streamId, errorMessage);
                return;
            }
            if (data != null) {
                bodyBuffer = Unpooled.wrappedBuffer(data);
            }
        } catch (Throwable e1) {
            // Note that this is a program logic error - some service isn't properly checking or setting Content-Type
            this.host.log(Level.SEVERE, "Error encoding body: %s", Utils.toString(e1));
            writeInternalServerError(ctx, request, streamId, "Error encoding body: " + e1.getMessage());
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

        if (request.hasResponseHeaders()) {
            // add any other custom headers associated with operation
            for (Entry<String, String> nameValue : request.getResponseHeaders().entrySet()) {
                response.headers().set(nameValue.getKey(), nameValue.getValue());
            }
        }

        // Add auth token to response if authorization context
        AuthorizationContext authorizationContext = request.getAuthorizationContext();
        if (authorizationContext != null && authorizationContext.shouldPropagateToClient()) {
            String token = authorizationContext.getToken();

            // The x-xenon-auth-token header is our preferred style
            response.headers().add(Operation.REQUEST_AUTH_TOKEN_HEADER, token);

            // Client can also use the cookie if they prefer
            StringBuilder buf = new StringBuilder()
                    .append(AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE)
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

    private void writeInternalServerError(ChannelHandlerContext ctx, Operation request, Integer streamId, String err) {
        byte[] data;
        try {
            data = err.getBytes(Utils.CHARSET);
        } catch (UnsupportedEncodingException ueex) {
            this.exceptionCaught(ctx, ueex);
            return;
        }

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                Unpooled.wrappedBuffer(data), false, false);
        if (streamId != null) {
            response.headers().setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(),
                    streamId);
        }
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, Operation.MEDIA_TYPE_TEXT_HTML);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        writeResponse(ctx, request, response);
        return;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Operation op = ctx.channel().attr(NettyChannelContext.OPERATION_KEY).get();
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
