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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.SocketContext;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.http.netty.NettyChannelPool.NettyChannelGroupKey;

public class NettyChannelContext extends SocketContext {
    static final Logger logger = Logger.getLogger(NettyChannelPool.class.getName());

    // For HTTP/1.1 channels, this stores the operation associated with the channel
    static final AttributeKey<Operation> OPERATION_KEY = AttributeKey
            .<Operation> valueOf("operation");
    // For HTTP/2 channels, the promise lets us know when the HTTP/2 settings
    // negotiation is complete
    static final AttributeKey<ChannelPromise> SETTINGS_PROMISE_KEY = AttributeKey
            .<ChannelPromise> valueOf("settings-promise");
    // For HTTP/2 connections, we store the channel context as an attribute on the
    // channel so we can get access to the context and stream mapping when needed
    static final AttributeKey<NettyChannelContext> CHANNEL_CONTEXT_KEY = AttributeKey
            .<NettyChannelContext> valueOf("channel-context");
    // The presence attribute tell us that a channel is using HTTP/2
    static final AttributeKey<Boolean> HTTP2_KEY = AttributeKey.<Boolean> valueOf("http2");

    public static final int BUFFER_SIZE = 4096 * 16;

    public static final int MAX_INITIAL_LINE_LENGTH = 4096;
    public static final int MAX_HEADER_SIZE = 65536;
    public static final int MAX_CHUNK_SIZE = 65536;
    public static final int MAX_HTTP2_FRAME_SIZE = 65536;
    public static final int INITIAL_HTTP2_WINDOW_SIZE = 65536;

    // An HTTP/2 connection uses a unique stream for each request/response
    // The stream ID is 31 bits longs and the client can only use odd-numbered
    // streams starting with stream 3. That gives us roughly a billion requests
    // before we have to close and reopen the connection
    public static final int DEFAULT_MAX_STREAM_ID = Integer.MAX_VALUE - 1;

    private static int maxStreamId = DEFAULT_MAX_STREAM_ID;

    // Indicates if this is being used for HTTP/1.2 or HTTP/2
    public enum Protocol {
        HTTP11, HTTP2
    }

    public static final PooledByteBufAllocator ALLOCATOR = NettyChannelContext.createAllocator();

    static PooledByteBufAllocator createAllocator() {
        // We are using defaults from the code internals since the pooled allocator does not
        // expose the values it calculates. The available constructor methods that take cache
        // sizes require us to pass things like max order and page size.
        // maxOrder determines the allocation chunk size as a multiple of page size
        int maxOrder = 4;
        return new PooledByteBufAllocator(true, 2, 2, 8192, maxOrder, 64, 32, 16);
    }

    private Channel channel;
    private final NettyChannelGroupKey key;
    private Protocol protocol;

    // An HTTP/2 connection may have multiple simultaneous operations. This map
    // Will associate each stream with the operation happening on the stream
    public final Map<Integer, Operation> streamIdMap;

    // We need to know if an HTTP/2 connection is being opened so that we can queue
    // pending operations instead of adding a new HTTP/2 connection
    private AtomicBoolean openInProgress = new AtomicBoolean(false);

    // We track the largest stream ID seen, so we know when the connection is exhausted
    private int largestStreamId = 0;

    private boolean isPoolStopping;

    public NettyChannelContext(NettyChannelGroupKey key, Protocol protocol) {
        this.key = key;
        this.protocol = protocol;
        if (protocol == Protocol.HTTP2) {
            this.streamIdMap = new HashMap<Integer, Operation>();
        } else {
            this.streamIdMap = null;
        }
    }

    public NettyChannelContext setChannel(Channel c) {
        this.channel = c;
        this.channel.closeFuture().addListener(future -> {
            if (this.isPoolStopping) {
                return;
            }
            logger.info("Channel closed" +
                    ", ChannelId:" + this.channel.id() +
                    ", Protocol:" + this.protocol +
                    ", NodeChannelGroupKey:" + this.key);
        });
        return this;
    }

    /**
     * Infrastructure use only: for testing purposes
     */
    public static void setMaxStreamId(int max) {
        maxStreamId = max;
    }

    public NettyChannelContext setOperation(Operation request) {
        if (this.channel == null) {
            return this;
        }
        if (this.streamIdMap == null) {
            this.channel.attr(OPERATION_KEY).set(request);
        }
        if (request == null) {
            return this;
        }
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

    public Operation getOperationForStream(int streamId) {
        if (this.streamIdMap == null) {
            throw new IllegalStateException(
                    "Internal error: requested operation for stream, but no records");
        }
        synchronized (this.streamIdMap) {
            return this.streamIdMap.get(streamId);
        }
    }

    public void setOperationForStream(int streamId, Operation operation) {
        if (this.streamIdMap == null) {
            throw new IllegalStateException(
                    "Internal error: Adding operation for stream, but no records");
        }
        synchronized (this.streamIdMap) {
            this.streamIdMap.put(streamId, operation);
            if (streamId > this.largestStreamId) {
                this.largestStreamId = streamId;
            }
        }
    }

    public void removeOperationForStream(int streamId) {
        if (this.streamIdMap == null) {
            return;
        }
        synchronized (this.streamIdMap) {
            this.streamIdMap.remove(streamId);
        }
    }

    public boolean hasActiveStreams() {
        synchronized (this.streamIdMap) {
            return !this.streamIdMap.isEmpty();
        }
    }

    public NettyChannelGroupKey getKey() {
        return this.key;
    }

    public Channel getChannel() {
        return this.channel;
    }

    public boolean isOpenInProgress() {
        return this.openInProgress.get();
    }

    public void setOpenInProgress(boolean inProgress) {
        this.openInProgress.set(inProgress);
    }

    public Protocol getProtocol() {
        return this.protocol;
    }

    /**
     * Returns true if we can't allocate any more streams. Used by NettyChannelPool
     * to decide when it's time to close the connection and open a new one.
     */
    public boolean isValid() {
        if (this.protocol == Protocol.HTTP11) {
            throw new IllegalStateException(
                    "Internal error: checked for stream exhaustion on HTTP/1.1 connection");
        } else {
            boolean isExhausted;
            synchronized (this.streamIdMap) {
                isExhausted = this.largestStreamId >= NettyChannelContext.maxStreamId;
            }
            return !isExhausted;
        }
    }

    public int getLargestStreamId() {
        return this.largestStreamId;
    }

    @Override
    public void writeHttpRequest(Object request) {
        this.channel.writeAndFlush(request);
        updateLastUseTime();
    }

    public void close(boolean isShutdown) {
        this.isPoolStopping = isShutdown;
        close();
    }

    @Override
    public void close() {
        Channel c = this.channel;
        this.openInProgress.set(false);
        if (c == null) {
            return;
        }

        if (c.isOpen()) {
            try {
                c.close();
            } catch (Throwable e) {
            }
        }

        Throwable e = new IllegalStateException("Socket channel closed:" + this.key);
        ServiceErrorResponse body = ServiceErrorResponse.createWithShouldRetry(e);

        Operation op = this.getOperation();
        if (op != null) {
            setOperation(null);
            op.setStatusCode(body.statusCode);
            op.fail(e, body);
            return;
        }

        if (this.streamIdMap == null || this.streamIdMap.isEmpty()) {
            return;
        }

        List<Operation> ops = new ArrayList<>();
        synchronized (this.streamIdMap) {
            ops.addAll(this.streamIdMap.values());
            this.streamIdMap.clear();
        }
        for (Operation o : ops) {
            o.setStatusCode(body.statusCode);
            o.fail(e, body);
        }
    }
}