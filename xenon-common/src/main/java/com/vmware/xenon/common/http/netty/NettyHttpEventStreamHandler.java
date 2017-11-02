/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.http.DefaultHttpObject;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServerSentEvent;
import com.vmware.xenon.common.serialization.ServerSentEventConverter;

/**
 * A handler that implements the EventStream protocol and translates the
 * {@link io.netty.handler.codec.http.HttpObject} contents into {@link ServerSentEvent}s
 */
public class NettyHttpEventStreamHandler extends DelimiterBasedFrameDecoder {
    private enum Phase {
        NOT_INITIALIZED,
        IN_EVENT_STREAM
    }

    private static final ByteBuf[] DELIMITERS = Stream.of(
                "\n\n",
                "\r\n\r\n")
            .map(d -> d.getBytes(ServerSentEventConverter.ENCODING_CHARSET))
            .map(Unpooled::wrappedBuffer)
            .toArray(ByteBuf[]::new);

    private Phase phase = Phase.NOT_INITIALIZED;

    public NettyHttpEventStreamHandler() {
        super(Integer.MAX_VALUE, DELIMITERS);
    }

    private boolean acceptInboundMessage(Object msg) throws Exception {
        if (msg instanceof FullHttpMessage) {
            return false;
        }

        switch (this.phase) {
        case NOT_INITIALIZED:
            if (msg instanceof HttpResponse) {
                return true;
            }
            break;
        case IN_EVENT_STREAM:
            if (msg instanceof HttpContent) {
                return true;
            }
            break;
        default:
            return false;
        }
        return false;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!acceptInboundMessage(msg)) {
            this.resetState();
            // Send to the next handler without changing
            ctx.fireChannelRead(msg);
            return;
        }

        switch (this.phase) {
        case NOT_INITIALIZED:
            HttpResponse response = (HttpResponse) msg;
            HttpHeaders headers = response.headers();
            String contentType = headers.get(Operation.CONTENT_TYPE_HEADER);
            if (Operation.MEDIA_TYPE_TEXT_EVENT_STREAM.equals(contentType)
                    && response.status().code() == Operation.STATUS_CODE_OK) {
                this.phase = Phase.IN_EVENT_STREAM;
            } else {
                this.resetState();
                ctx.fireChannelRead(msg);
                return;
            }
            EventStreamHeadersMessage transformedMsg = new EventStreamHeadersMessage();
            transformedMsg.originalResponse = response;
            ctx.fireChannelRead(transformedMsg);
            return;
        case IN_EVENT_STREAM:
            ByteBuf content = ((HttpContent) msg).content();
            super.channelRead(ctx, content);
            return;
        default:
            this.resetState();
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        ByteBuf rawEvent = (ByteBuf) super.decode(ctx, buffer);
        if (rawEvent == null) {
            return null;
        }
        String serializedEvent = rawEvent.toString(ServerSentEventConverter.ENCODING_CHARSET);
        rawEvent.release();
        ServerSentEvent event = ServerSentEventConverter.INSTANCE.deserialize(serializedEvent);
        EventStreamMessage message = new EventStreamMessage();
        message.event = event;
        return message;
    }

    private void resetState() {
        this.phase = Phase.NOT_INITIALIZED;
    }

    /**
     * Wraps a HTTPObject
     */
    static final class EventStreamMessage extends DefaultHttpObject {
        ServerSentEvent event;

        @Override
        public boolean equals(Object o) {
            return this == o;
        }
    }

    static final class EventStreamHeadersMessage extends DefaultHttpObject {
        HttpResponse originalResponse;

        @Override
        public boolean equals(Object o) {
            return this == o;
        }
    }
}
