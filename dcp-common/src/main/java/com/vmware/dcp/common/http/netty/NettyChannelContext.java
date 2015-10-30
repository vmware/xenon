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

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.SocketContext;

public class NettyChannelContext extends SocketContext {
    static final AttributeKey<Operation> OPERATION_KEY = AttributeKey
            .<Operation> valueOf("operation");
    public static final int BUFFER_SIZE = 4096 * 16;

    public static final PooledByteBufAllocator ALLOCATOR = NettyChannelContext.createAllocator();

    static PooledByteBufAllocator createAllocator() {
        // We are using defaults from the code internals since the pooled allocator does not
        // expose the values it calculates. The available constructor methods that take cache
        // sizes require us to pass things like max order and page size.
        // maxOrder determines the allocation chunk size as a multiple of page size
        int maxOrder = 4;
        return new PooledByteBufAllocator(true, 2, 2, 8192, maxOrder, 64, 32, 16);
    }

    int port;
    String host;
    private Channel channel;
    private final String key;

    public NettyChannelContext(String host, int port, String key) {
        this.host = host;
        this.port = port;
        this.key = key;
    }

    public NettyChannelContext setChannel(Channel c) {
        this.channel = c;
        return this;
    }

    public NettyChannelContext setOperation(Operation request) {
        this.channel.attr(OPERATION_KEY).set(request);
        request.setSocketContext(this);
        return this;
    }

    public Operation getOperation() {
        Channel ch = this.channel;
        if (ch == null) {
            return null;
        }
        return ch.attr(OPERATION_KEY).get();
    }

    public String getKey() {
        return this.key;
    }

    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public void writeHttpRequest(Object request) {
        this.channel.writeAndFlush(request);
        updateLastUseTime();
    }

    @Override
    public void close() {
        Channel c = this.channel;
        if (c == null) {
            return;
        }
        if (!c.isOpen()) {
            return;
        }
        try {
            c.close();
        } catch (Throwable e) {
        }
    }
}