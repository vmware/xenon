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

package com.vmware.dcp.common.http.netty;

import java.net.ProtocolException;
import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderUtil;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceErrorResponse;
import com.vmware.dcp.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.dcp.common.Utils;

/**
 * Processes responses from a remote HTTP server and completes the request associated with the
 * channel
 */
public class NettyHttpServerResponseHandler extends SimpleChannelInboundHandler<HttpObject> {

    private NettyChannelPool pool;

    public NettyHttpServerResponseHandler(NettyChannelPool pool) {
        this.pool = pool;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, HttpObject msg) {
        Operation request = ctx.channel().attr(NettyChannelContext.OPERATION_KEY).get();
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            request.setStatusCode(response.status().code());
            parseResponseHeaders(request, response);
            completeRequest(ctx, request, response.content());
        }
    }

    private void parseResponseHeaders(Operation request, HttpResponse nettyResponse) {
        HttpHeaders headers = nettyResponse.headers();
        if (headers.isEmpty()) {
            return;
        }

        request.setKeepAlive(HttpHeaderUtil.isKeepAlive(nettyResponse));
        if (HttpHeaderUtil.isContentLengthSet(nettyResponse)) {
            request.setContentLength(HttpHeaderUtil.getContentLength(nettyResponse));
        }

        for (Entry<String, String> h : headers.entriesConverted()) {
            String key = h.getKey();
            String value = h.getValue();

            if (key.equalsIgnoreCase(HttpHeaderNames.CONTENT_TYPE.toString())) {
                request.setContentType(value);
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
            request.setContentLength(0)
                    .setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
            request.complete();
            return;
        }

        request.nestCompletion((o, e) -> {
            if (e != null) {
                request.fail(e);
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
        Logger.getAnonymousLogger().warning(Utils.toString(cause));
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

    private boolean checkResponseForError(Operation op) {
        if (op.getStatusCode() < Operation.STATUS_CODE_FAILURE_THRESHOLD) {
            return false;
        }
        String errorMsg = String.format("Service %s returned error %d for %s",
                op.getUri(), op.getStatusCode(), op.getAction());

        if (!op.hasBody()) {
            ServiceErrorResponse rsp = ServiceErrorResponse.create(new ProtocolException(errorMsg),
                    op.getStatusCode());
            op.setBodyNoCloning(rsp);
        }

        op.fail(new ProtocolException(errorMsg));
        return true;
    }
}