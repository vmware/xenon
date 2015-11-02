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

import java.util.logging.Level;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.util.internal.logging.InternalLogLevel.INFO;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

//import io.netty.handler.codec.AsciiString;
//import io.netty.handler.codec.http.DefaultFullHttpRequest;
//import io.netty.handler.codec.http.FullHttpRequest;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceHost;

public class NettyHttp2ClientRequestHandler extends Http2ConnectionHandler {
    private static final Http2FrameLogger logger = new Http2FrameLogger(INFO,
            InternalLoggerFactory.getInstance(NettyHttp2ClientRequestHandler.class));
    private ServiceHost host;

    public NettyHttp2ClientRequestHandler(ServiceHost host) {
        this(new DefaultHttp2Connection(true),
                new Http2InboundFrameLogger(new DefaultHttp2FrameReader(), logger),
                new Http2OutboundFrameLogger(new DefaultHttp2FrameWriter(), logger),
                new NettyHttp2FrameListener());

        this.host = host;
    }

    private NettyHttp2ClientRequestHandler(Http2Connection connection, Http2FrameReader frameReader,
                                           Http2FrameWriter frameWriter, NettyHttp2FrameListener listener) {
        super(connection, frameReader, frameWriter, listener);
        listener.encoder(encoder());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof HttpServerUpgradeHandler.UpgradeEvent) {
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Operation op = ctx.attr(NettyChannelContext.OPERATION_KEY).get();
        if (op != null) {
            this.host.log(Level.SEVERE, "Listener channel exception: %s, in progress op: %s", cause.getMessage(),
                    op.toString());
        }
        ctx.channel().attr(NettyChannelContext.OPERATION_KEY).remove();
        ctx.close();
    }

    public static class NettyHttp2FrameListener extends Http2FrameAdapter {
        private Http2ConnectionEncoder encoder;

        public void encoder(Http2ConnectionEncoder encoder) {
            this.encoder = encoder;
        }

        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                              boolean endOfStream) throws Http2Exception {
            int processed = data.readableBytes() + padding;
//            if (endOfStream) {
//                FullHttpRequest request = new DefaultFullHttpRequest();
//                request.copy(data);
//
//                sendResponse(ctx, streamId, data.retain());
//            }
            return processed;
        }

        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
                                  Http2Headers headers, int streamDependency, short weight,
                                  boolean exclusive, int padding, boolean endStream) throws Http2Exception {
            if (endStream) {
//                sendResponse(ctx, streamId, RESPONSE_BYTES.duplicate());
            }
        }

        private void sendResponse(ChannelHandlerContext ctx, int streamId, ByteBuf payload) {
            // Send a frame for the response status
            Http2Headers headers = new DefaultHttp2Headers().status(OK.codeAsText());
            this.encoder.writeHeaders(ctx, streamId, headers, 0, false, ctx.newPromise());
            this.encoder.writeData(ctx, streamId, payload, 0, true, ctx.newPromise());
            ctx.flush();
        }
    }
}
