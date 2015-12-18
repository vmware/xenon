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

import java.util.logging.Level;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpClientUpgradeHandler;

import com.vmware.xenon.common.Utils;

/**
 * This is a handler that is inserted into a Netty channel pipeline just to log
 * user events related to upgrades from HTTP/1.1 to HTTP/2. We only log failed
 * attempts (that's interesting) unless debugLogging is enabled, in which case
 * we log all events.
 */
public class NettyHttp2UserEventLogger extends ChannelInboundHandlerAdapter {

    private boolean debugLogging = false;

    public NettyHttp2UserEventLogger(boolean debugLogging) {
        this.debugLogging = debugLogging;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext context, Object eventRaw)
            throws Exception {
        if (!(eventRaw instanceof HttpClientUpgradeHandler.UpgradeEvent)) {
            super.userEventTriggered(context, eventRaw);
            return;
        }

        HttpClientUpgradeHandler.UpgradeEvent event = (HttpClientUpgradeHandler.UpgradeEvent) eventRaw;
        if (event == HttpClientUpgradeHandler.UpgradeEvent.UPGRADE_REJECTED) {
            Utils.log(NettyHttpClientRequestInitializer.class,
                    NettyHttpClientRequestInitializer.class.getSimpleName(),
                    Level.WARNING, "Failed to upgrade to HTTP/2: throughput will be slow");
        } else if (this.debugLogging) {
            Utils.log(NettyHttpClientRequestInitializer.class,
                    NettyHttpClientRequestInitializer.class.getSimpleName(),
                    Level.INFO, "HTTP/2 channel pipeline user event: %s", event);
        }
        super.userEventTriggered(context, eventRaw);
    }
}
