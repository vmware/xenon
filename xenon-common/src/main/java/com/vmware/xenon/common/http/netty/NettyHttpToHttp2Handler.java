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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.EmptyHttp2Headers;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.ReferenceCountUtil;

import com.vmware.xenon.common.Operation;

/**
 * Translates HTTP/1.x object writes into HTTP/2 frames. It was copied from the Netty source
 * and has been updated to record the association between an HTTP/2 stream ID and our
 * operation, so we can properly handle responses.
 */
public class NettyHttpToHttp2Handler extends Http2ConnectionHandler {
    private final boolean validateHeaders;
    private int currentStreamId;

    /**
     * Builder which builds {@link NettyHttpToHttp2Handler} objects.
     */
    public static final class Builder extends BuilderBase<NettyHttpToHttp2Handler, Builder> {
        @Override
        public NettyHttpToHttp2Handler build0(Http2ConnectionDecoder decoder,
                Http2ConnectionEncoder encoder) {
            return new NettyHttpToHttp2Handler(decoder, encoder, initialSettings(),
                    isValidateHeaders());
        }
    }

    protected NettyHttpToHttp2Handler(Http2ConnectionDecoder decoder,
            Http2ConnectionEncoder encoder,
            Http2Settings initialSettings, boolean validateHeaders) {
        super(decoder, encoder, initialSettings);
        this.validateHeaders = validateHeaders;
    }

    /**
     * Get the next stream id either from the {@link HttpHeaders} object or HTTP/2 codec
     *
     * @param httpHeaders The HTTP/1.x headers object to look for the stream id
     * @return The stream id to use with this {@link HttpHeaders} object
     * @throws Exception If the {@code httpHeaders} object specifies an invalid stream id
     */
    private int getStreamId(HttpHeaders httpHeaders) throws Exception {
        return httpHeaders.getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(),
                connection().local().nextStreamId());
    }

    /**
     * Handles conversion of {@link HttpMessage} and {@link HttpContent} to HTTP/2 frames.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {

        if (!(msg instanceof HttpMessage || msg instanceof HttpContent)) {
            ctx.write(msg, promise);
            return;
        }

        boolean release = true;
        SimpleChannelPromiseAggregator promiseAggregator = new SimpleChannelPromiseAggregator(
                promise, ctx.channel(), ctx.executor());
        try {
            Http2ConnectionEncoder encoder = encoder();
            boolean endStream = false;
            if (msg instanceof HttpMessage) {
                final HttpMessage httpMsg = (HttpMessage) msg;

                // Provide the user the opportunity to specify the streamId
                this.currentStreamId = getStreamId(httpMsg.headers());

                // Record the association between the operation and the
                // streamId, so we can process the response.
                if (msg instanceof NettyFullHttpRequest) {
                    NettyFullHttpRequest request = (NettyFullHttpRequest) msg;
                    Operation operation = request.getOperation();
                    if (operation != null) {
                        NettyChannelContext socketContext = (NettyChannelContext) operation.getSocketContext();
                        if (socketContext != null) {
                            socketContext.setOperationForStream(this.currentStreamId, operation);
                        }
                    }
                }

                // Convert and write the headers.
                Http2Headers http2Headers = HttpConversionUtil.toHttp2Headers(httpMsg,
                        this.validateHeaders);
                endStream = msg instanceof FullHttpMessage
                        && !((FullHttpMessage) msg).content().isReadable();
                encoder.writeHeaders(ctx, this.currentStreamId, http2Headers, 0, endStream,
                        promiseAggregator.newPromise());
            }

            if (!endStream && msg instanceof HttpContent) {
                boolean isLastContent = false;
                Http2Headers trailers = EmptyHttp2Headers.INSTANCE;
                if (msg instanceof LastHttpContent) {
                    isLastContent = true;

                    // Convert any trailing headers.
                    final LastHttpContent lastContent = (LastHttpContent) msg;
                    trailers = HttpConversionUtil.toHttp2Headers(lastContent.trailingHeaders(),
                            this.validateHeaders);
                }

                // Write the data
                final ByteBuf content = ((HttpContent) msg).content();
                endStream = isLastContent && trailers.isEmpty();
                release = false;
                encoder.writeData(ctx, this.currentStreamId, content, 0, endStream,
                        promiseAggregator.newPromise());

                if (!trailers.isEmpty()) {
                    // Write trailing headers.
                    encoder.writeHeaders(ctx, this.currentStreamId, trailers, 0, true,
                            promiseAggregator.newPromise());
                }
            }

            promiseAggregator.doneAllocatingPromises();
        } catch (Throwable t) {
            promiseAggregator.setFailure(t);
        } finally {
            if (release) {
                ReferenceCountUtil.release(msg);
            }
        }
    }

}