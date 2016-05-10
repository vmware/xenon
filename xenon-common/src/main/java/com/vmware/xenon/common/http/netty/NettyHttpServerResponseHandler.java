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

import java.net.ProtocolException;
import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.HttpConversionUtil;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.xenon.common.Utils;

/**
 * Processes responses from a remote HTTP server and completes the request associated with the
 * channel
 */
public class NettyHttpServerResponseHandler extends SimpleChannelInboundHandler<HttpObject> {

    private NettyChannelPool pool;
    private Logger logger = Logger.getLogger(getClass().getName());

    public NettyHttpServerResponseHandler(NettyChannelPool pool) {
        this.pool = pool;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;

            Operation request = findOperation(ctx, response);
            request.setStatusCode(response.status().code());
            parseResponseHeaders(request, response);
            completeRequest(ctx, request, response.content());
        }
    }

    /**
     * We find the operation differently for HTTP/1.1 and HTTP/2
     *
     * For HTTP/1.1, there is only one request per channel, and it's stored as an attribute
     * on the channel
     *
     * For HTTP/2, we have multiple requests and have to check a map in the associated
     * NettyChannelContext
     */
    private Operation findOperation(ChannelHandlerContext ctx, FullHttpResponse response) {
        Operation request;

        if (ctx.channel().hasAttr(NettyChannelContext.HTTP2_KEY)) {
            Integer streamId = response.headers()
                    .getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
            if (streamId == null) {
                this.logger.warning("HTTP/2 message has no stream ID: ignoring.");
                return null;
            }
            NettyChannelContext channelContext = ctx.channel().attr(NettyChannelContext.CHANNEL_CONTEXT_KEY).get();
            if (channelContext == null) {
                this.logger.warning(
                        "HTTP/2 channel is missing associated channel context: ignoring response on stream "
                                + streamId);
                return null;
            }
            request = channelContext.getOperationForStream(streamId);
            if (request == null) {
                this.logger.warning("Can't find operation for stream " + streamId);
                return null;
            }
            // We only have one request/response per stream, so remove the association.
            channelContext.removeOperationForStream(streamId);
        } else {
            request = ctx.channel().attr(NettyChannelContext.OPERATION_KEY).get();
            if (request == null) {
                this.logger.warning("Can't find operation for channel");
                return null;
            }
        }
        return request;
    }

    private void parseResponseHeaders(Operation request, HttpResponse nettyResponse) {
        HttpHeaders headers = nettyResponse.headers();
        if (headers.isEmpty()) {
            return;
        }

        request.setKeepAlive(HttpUtil.isKeepAlive(nettyResponse));
        if (HttpUtil.isContentLengthSet(nettyResponse)) {
            request.setContentLength(HttpUtil.getContentLength(nettyResponse));
            headers.remove(HttpHeaderNames.CONTENT_LENGTH);
        }

        String contentType = headers.get(HttpHeaderNames.CONTENT_TYPE);
        headers.remove(HttpHeaderNames.CONTENT_TYPE);
        if (contentType != null) {
            request.setContentType(contentType);
        }

        for (Entry<String, String> h : headers) {
            String key = h.getKey();
            String value = h.getValue();
            if (Operation.STREAM_ID_HEADER.equals(key)) {
                // Prevent allocation of response headers in Operation and hide the stream ID
                // header, since it is manipulated by the HTTP layer, not services
                continue;
            }
            request.addResponseHeader(key, value);
        }
    }

    private void completeRequest(ChannelHandlerContext ctx, Operation request, ByteBuf content) {
        decodeResponseBody(request, content);
        this.pool.returnOrClose((NettyChannelContext) request.getSocketContext(),
                !request.isKeepAlive());
    }

    private void decodeResponseBody(Operation request, ByteBuf content) {
        if (!content.isReadable()) {
            if (checkResponseForError(request)) {
                return;
            }
            // skip body decode, request had no body
            request.setContentLength(0).complete();
            return;
        }

        request.nestCompletion((o, e) -> {
            if (e != null) {
                request.fail(e);
                return;
            }
            if (checkResponseForError(request)) {
                return;
            }
            completeRequest(request);
        });

        Utils.decodeBody(request, content.nioBuffer());
    }

    private void completeRequest(Operation request) {
        if (checkResponseForError(request)) {
            return;
        }
        request.complete();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Operation request = ctx.channel().attr(NettyChannelContext.OPERATION_KEY).get();

        if (request == null) {
            // This will happen when using HTTP/2 because we have multiple requests
            // associated with the channel. For now, we're just logging, but we could
            // find all the requests and fail all of them. That's slightly risky because
            // we don't understand why we failed, and we may get responses for them later.
            this.logger.info(
                    "Channel exception but no HTTP/1.1 request to fail:" + cause.getMessage());
            return;
        }

        // I/O exception this code recommends retry since it never made it to the remote end
        request.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST);
        request.setBody(ServiceErrorResponse.create(cause, request.getStatusCode(),
                EnumSet.of(ErrorDetail.SHOULD_RETRY)));
        request.fail(cause);
    }

    private boolean checkResponseForError(Operation op) {
        if (op.getStatusCode() < Operation.STATUS_CODE_FAILURE_THRESHOLD) {
            return false;
        }
        String errorMsg = String.format("Service %s returned error %d for %s. id %d",
                op.getUri(), op.getStatusCode(), op.getAction(), op.getId());

        if (!op.hasBody()) {
            ServiceErrorResponse rsp = ServiceErrorResponse.create(new ProtocolException(errorMsg),
                    op.getStatusCode());
            op.setBodyNoCloning(rsp);
        } else if (Operation.MEDIA_TYPE_APPLICATION_JSON.equals(op.getContentType())) {
            Object originalBody = op.getBodyRaw();
            ServiceErrorResponse rsp = op.getBody(ServiceErrorResponse.class);
            if (rsp != null) {
                errorMsg += " message " + rsp.message;
            }
            op.setBodyNoCloning(originalBody);
        }

        op.fail(new ProtocolException(errorMsg));
        return true;
    }

}
