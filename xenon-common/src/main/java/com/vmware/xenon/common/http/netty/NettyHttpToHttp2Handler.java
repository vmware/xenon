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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpUtil;

import com.vmware.xenon.common.Operation;

/**
 * Translates HTTP/1.x object writes into HTTP/2 frames. It was copied from the Netty source
 * and has been updated to record the association between an HTTP/2 stream ID and our
 * operation, so we can properly handle responses.
 */
public class NettyHttpToHttp2Handler extends HttpToHttp2ConnectionHandler {
    public NettyHttpToHttp2Handler(boolean server, Http2FrameListener listener) {
        super(server, listener);
    }

    public NettyHttpToHttp2Handler(Http2Connection connection, Http2FrameListener listener) {
        super(connection, listener);
    }

    public NettyHttpToHttp2Handler(Http2Connection connection, Http2FrameReader frameReader,
            Http2FrameWriter frameWriter, Http2FrameListener listener) {
        super(connection, frameReader, frameWriter, listener);
    }

    public NettyHttpToHttp2Handler(Http2ConnectionDecoder.Builder decoderBuilder,
            Http2ConnectionEncoder.Builder encoderBuilder) {
        super(decoderBuilder, encoderBuilder);
    }

    /**
     * Get the next stream id either from the {@link HttpHeaders} object or HTTP/2 codec
     *
     * @param httpHeaders The HTTP/1.x headers object to look for the stream id
     * @return The stream id to use with this {@link HttpHeaders} object
     * @throws Exception If the {@code httpHeaders} object specifies an invalid stream id
     */
    private int getStreamId(HttpHeaders httpHeaders) throws Exception {
        return httpHeaders.getInt(HttpUtil.ExtensionHeaderNames.STREAM_ID.text(), connection().local().nextStreamId());
    }

    /**
     * Handles conversion of a {@link FullHttpMessage} to HTTP/2 frames.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof FullHttpMessage) {
            FullHttpMessage httpMsg = (FullHttpMessage) msg;
            boolean hasData = httpMsg.content().isReadable();
            boolean httpMsgNeedRelease = true;
            SimpleChannelPromiseAggregator promiseAggregator = null;
            try {
                // Provide the user the opportunity to specify the streamId
                int streamId = getStreamId(httpMsg.headers());

                // Record the association between the operation and the
                // streamId, so we can process the response.
                if (msg instanceof NettyFullHttpRequest) {
                    NettyFullHttpRequest request = (NettyFullHttpRequest) msg;
                    Operation operation = request.getOperation();
                    if (operation != null) {
                        NettyChannelContext socketContext = (NettyChannelContext) operation.getSocketContext();
                        if (socketContext != null) {
                            socketContext.setOperationForStream(streamId, operation);
                        }
                    }
                }

                // Convert and write the headers.
                Http2Headers http2Headers = HttpUtil.toHttp2Headers(httpMsg);
                Http2ConnectionEncoder encoder = encoder();

                if (hasData) {
                    promiseAggregator = new SimpleChannelPromiseAggregator(promise, ctx.channel(), ctx.executor());
                    encoder.writeHeaders(ctx, streamId, http2Headers, 0, false, promiseAggregator.newPromise());
                    httpMsgNeedRelease = false;
                    encoder.writeData(ctx, streamId, httpMsg.content(), 0, true, promiseAggregator.newPromise());
                    promiseAggregator.doneAllocatingPromises();
                } else {
                    encoder.writeHeaders(ctx, streamId, http2Headers, 0, true, promise);
                }
            } catch (Throwable t) {
                if (promiseAggregator == null) {
                    promise.tryFailure(t);
                } else {
                    promiseAggregator.setFailure(t);
                }
            } finally {
                if (httpMsgNeedRelease) {
                    httpMsg.release();
                }
            }
        } else {
            ctx.write(msg, promise);
        }
    }
}