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

import javax.net.ssl.SSLEngine;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2OrHttpChooser;

import com.vmware.dcp.common.ServiceHost;

public class NettyHttp2OrHttpClientRequestHandler extends Http2OrHttpChooser {
    private static final int MAX_CONTENT_LENGTH = 1024 * 100;
    private ServiceHost host;

    public NettyHttp2OrHttpClientRequestHandler(ServiceHost host) {
        this(MAX_CONTENT_LENGTH);

        this.host = host;
    }

    public NettyHttp2OrHttpClientRequestHandler(ServiceHost host, int maxHttpContentLength) {
        super(maxHttpContentLength);

        this.host = host;
    }

    protected NettyHttp2OrHttpClientRequestHandler(int maxHttpContentLength) {
        super(maxHttpContentLength);
    }

    @Override
    protected SelectedProtocol getProtocol(SSLEngine engine) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected ChannelHandler createHttp1RequestHandler() {
        return new NettyHttpClientRequestHandler(this.host);
    }

    @Override
    protected Http2ConnectionHandler createHttp2RequestHandler() {
        return new NettyHttp2ClientRequestHandler(this.host);
    }

}
