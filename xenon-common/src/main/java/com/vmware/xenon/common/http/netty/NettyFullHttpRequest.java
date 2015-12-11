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
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import com.vmware.xenon.common.Operation;

/**
 * We override DefaultFullHttpRequest so that later (in XXX) we can figure out what
 * streamId to associate with our Operation, so that we can call the call the correct
 * completion handler when we get a response.
 *
 * Much of the code in here is copied from DefaultFullHttpRequest.
 *
 */
public class NettyFullHttpRequest extends DefaultFullHttpRequest {

    private Operation operation;

    public NettyFullHttpRequest(HttpVersion httpVersion, HttpMethod method, String uri) {
        super(httpVersion, method, uri);
    }

    public NettyFullHttpRequest(HttpVersion httpVersion, HttpMethod method, String uri, ByteBuf content) {
        super(httpVersion, method, uri, content);
    }

    public NettyFullHttpRequest(HttpVersion httpVersion, HttpMethod method, String uri,
                                  ByteBuf content, boolean validateHeaders) {
        super(httpVersion, method, uri, content, validateHeaders);
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return this.operation;
    }

    private FullHttpRequest copy(boolean copyContent, ByteBuf newContent) {
        NettyFullHttpRequest copy = new NettyFullHttpRequest(
                protocolVersion(),
                method(),
                uri(),
                copyContent ? content().copy() :
                    newContent == null ? Unpooled.buffer(0) : newContent);
        copy.headers().set(headers());
        copy.trailingHeaders().set(trailingHeaders());
        return copy;
    }

    @Override
    public FullHttpRequest copy(ByteBuf newContent) {
        return copy(false, newContent);
    }

    @Override
    public FullHttpRequest copy() {
        return copy(true, null);
    }

    @Override
    public FullHttpRequest duplicate() {
        DefaultFullHttpRequest duplicate = new NettyFullHttpRequest(
                protocolVersion(), method(), uri(), content().duplicate());
        duplicate.headers().set(headers());
        duplicate.trailingHeaders().set(trailingHeaders());
        return duplicate;
    }

}