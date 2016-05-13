/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import java.lang.reflect.Field;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http2.Http2Connection.Endpoint;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2LocalFlowController;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ReflectionUtils;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.Utils;

/**
 * Translates HTTP/1.x object writes into HTTP/2 frames.
 *
 * Extend original netty handler to record the association between an HTTP/2 stream ID and our
 * operation, so we can properly handle responses.
 */
public class NettyHttpToHttp2Handler extends HttpToHttp2ConnectionHandler {

    public NettyHttpToHttp2Handler(Http2ConnectionDecoder decoder,
            Http2ConnectionEncoder encoder,
            Http2Settings initialSettings, boolean validateHeaders) {
        super(decoder, encoder, initialSettings, validateHeaders);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        // xenon custom logic (this does not modify the state of streamId)
        associateOperationAndStreamId(msg);

        // delegate to netty logic
        super.write(ctx, msg, promise);
    }

    private void associateOperationAndStreamId(Object msg) {
        if (!(msg instanceof NettyFullHttpRequest)) {
            return;
        }

        NettyFullHttpRequest request = (NettyFullHttpRequest) msg;
        Operation operation = request.getOperation();
        if (operation == null) {
            return;
        }

        int currentStreamId;
        try {
            currentStreamId = getStreamId(request.headers());
        } catch (Exception ex) {
            Utils.logWarning("Failed to retrieve streamId: %s", Utils.toString(ex));
            operation.fail(new RuntimeException("Failed to retrieve streamId", ex));
            return;
        }

        NettyChannelContext socketContext = (NettyChannelContext) operation.getSocketContext();
        if (socketContext == null) {
            return;
        }

        Operation oldOperation = socketContext.getOperationForStream(currentStreamId);

        if (oldOperation == null || oldOperation.getId() == operation.getId()) {
            socketContext.setOperationForStream(currentStreamId, operation);
            return;
        }
        long oldOpId = oldOperation.getId();
        long opId = operation.getId();
        // Reusing stream should NOT happen. sign for serious error...
        Utils.logWarning("Reusing stream %d. opId=%d, oldOpId=%d",
                currentStreamId, opId, oldOpId);
        Throwable e = new IllegalStateException("HTTP/2 Stream ID collision for id "
                + currentStreamId);
        ServiceErrorResponse rsp = ServiceErrorResponse.createWithShouldRetry(e);

        oldOperation.setRetryCount(1);
        operation.setRetryCount(1);

        // fail both operations, close the channel
        socketContext.setOperation(null);
        socketContext.removeOperationForStream(currentStreamId);
        socketContext.close();
        oldOperation.setBodyNoCloning(rsp).fail(e, rsp.statusCode);
        operation.setBodyNoCloning(rsp).fail(e, rsp.statusCode);
    }

    /**
     * Analogous to the {@link HttpToHttp2ConnectionHandler#getStreamId}.
     * However, behavior has changed NOT to modify the state of the instance since original
     * method changes state(increments nextReservationStreamId) and is called at
     * {@link #write(ChannelHandlerContext, Object, ChannelPromise)} subsequent to this method.
     */
    private int getStreamId(HttpHeaders httpHeaders) {
        // when streamId in header is invalid(invalid format,etc), this throws exception
        Integer streamId = httpHeaders
                .getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());

        if (streamId != null) {
            return streamId;
        }
        return getDefaultStreamId();
    }

    /**
     * Extracted logic from DefaultHttp2Connection.DefaultEndpoint#incrementAndGetNextStreamId()
     *
     * Original logic increments the 'nextReservationStreamId' which changes the state of instance.
     * In this method, only mimic the behavior but do NOT increment the value to avoid changing the
     * state since original logic is called at {@link #write(ChannelHandlerContext, Object, ChannelPromise)}.
     *
     * @see io.netty.handler.codec.http2.DefaultHttp2Connection.DefaultEndpoint#incrementAndGetNextStreamId()
     * @see HttpToHttp2ConnectionHandler#getStreamId(io.netty.handler.codec.http.HttpHeaders)
     */
    private int getDefaultStreamId() {
        Endpoint<Http2LocalFlowController> endpoint = connection().local();
        // unfortunately, need to retrieve value via reflection
        Field field = ReflectionUtils.getField(endpoint.getClass(), "nextReservationStreamId");

        try {

            int nextReservationStreamId = field.getInt(endpoint);
            return nextReservationStreamId >= 0 ?
                    nextReservationStreamId + 2 :
                    nextReservationStreamId;

        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
