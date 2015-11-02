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

import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http2.Http2Settings;

/**
 * Processes client requests on behalf of the HTTP listener and submits them to the service host or websocket client for
 * processing
 */
class NettyHttp2ClientSettingsHandler extends SimpleChannelInboundHandler<Http2Settings> {
    private ChannelPromise promise;

    public NettyHttp2ClientSettingsHandler(ChannelPromise promise) {
        this.promise = promise;
    }

    public void awaitSettings(long timeout, TimeUnit unit) throws Exception {
        if (!this.promise.awaitUninterruptibly(timeout, unit)) {
            throw new IllegalStateException("Timed out waiting for settings");
        }
        if (!this.promise.isSuccess()) {
            throw new RuntimeException(this.promise.cause());
        }
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, Http2Settings msg) throws Exception {
        this.promise.setSuccess();

        // Only care about the first settings message
        ctx.pipeline().remove(this);
    }
}
