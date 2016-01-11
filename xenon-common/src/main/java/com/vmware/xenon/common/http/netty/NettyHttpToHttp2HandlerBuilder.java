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

import io.netty.handler.codec.http2.AbstractHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2HeadersEncoder.SensitivityDetector;
import io.netty.handler.codec.http2.Http2Settings;

/**
 * Based on HttpToHttp2ConnectionHandlerBuilder.java from Netty
 */
public final class NettyHttpToHttp2HandlerBuilder extends
        AbstractHttp2ConnectionHandlerBuilder<NettyHttpToHttp2Handler, NettyHttpToHttp2HandlerBuilder> {

    @Override
    public NettyHttpToHttp2HandlerBuilder validateHeaders(boolean validateHeaders) {
        return super.validateHeaders(validateHeaders);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder initialSettings(Http2Settings settings) {
        return super.initialSettings(settings);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder frameListener(Http2FrameListener frameListener) {
        return super.frameListener(frameListener);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder gracefulShutdownTimeoutMillis(
            long gracefulShutdownTimeoutMillis) {
        return super.gracefulShutdownTimeoutMillis(gracefulShutdownTimeoutMillis);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder server(boolean isServer) {
        return super.server(isServer);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder connection(Http2Connection connection) {
        return super.connection(connection);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder codec(Http2ConnectionDecoder decoder,
            Http2ConnectionEncoder encoder) {
        return super.codec(decoder, encoder);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder frameLogger(Http2FrameLogger frameLogger) {
        return super.frameLogger(frameLogger);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder encoderEnforceMaxConcurrentStreams(
            boolean encoderEnforceMaxConcurrentStreams) {
        return super.encoderEnforceMaxConcurrentStreams(encoderEnforceMaxConcurrentStreams);
    }

    @Override
    public NettyHttpToHttp2HandlerBuilder headerSensitivityDetector(
            SensitivityDetector headerSensitivityDetector) {
        return super.headerSensitivityDetector(headerSensitivityDetector);
    }

    @Override
    public NettyHttpToHttp2Handler build() {
        return super.build();
    }

    @Override
    protected NettyHttpToHttp2Handler build(Http2ConnectionDecoder decoder,
            Http2ConnectionEncoder encoder,
            Http2Settings initialSettings) {
        return new NettyHttpToHttp2Handler(decoder, encoder, initialSettings,
                isValidateHeaders());
    }
}
