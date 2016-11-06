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

package com.vmware.xenon.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.serialization.KryoSerializers;

/**
 * infrastructure use only.
 */
public final class BodyEncoder {
    public static final String ERROR_MSG_DECODING_FAILURE = "Failure decoding HTTP request";

    private static final int INITIAL_CAPACITY = 8 * 1024;

    private BodyEncoder() {

    }

    private static ByteBuf allocate(boolean pool, int maxCapacity) {
        if (pool) {
            return pooledAllocator().directBuffer(maxCapacity, maxCapacity);
        } else {
            return Unpooled.buffer(maxCapacity);
        }
    }

    private static ByteBuf allocate(boolean pool) {
        if (pool) {
            return pooledAllocator().directBuffer(INITIAL_CAPACITY);
        } else {
            return Unpooled.buffer(INITIAL_CAPACITY);
        }
    }

    private static PooledByteBufAllocator pooledAllocator() {
        return PooledByteBufAllocator.DEFAULT;
    }

    /**
     * return a direct buffer when there is a body or an empty memory unpooleed buffer othwerwise,
     * never null.
     * While encoding it will also set missing http headers on the operation.
     *
     * @param op
     * @param body
     * @param contentType
     * @return
     */
    public static ByteBuf encodeBody(Operation op, Object body, String contentType) {
        return encodeBody(op, body, contentType, true);
    }

    private static ByteBuf encodeBody(Operation op, Object body, String contentType, boolean pool) {
        if (body == null) {
            op.setContentLength(0);
            return Unpooled.EMPTY_BUFFER;
        }

        if (body instanceof String) {
            String s = (String) body;
            ByteBuf buf = allocate(pool);
            buf.writeCharSequence(s, CharsetUtil.UTF_8);
            op.setContentLength(buf.writerIndex());
            return buf;
        } else if (body instanceof byte[]) {
            byte[] data = (byte[]) body;
            if (contentType == null) {
                op.setContentType(Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM);
            }
            if (op.getContentLength() == 0 || op.getContentLength() > data.length) {
                op.setContentLength(data.length);
            }
            ByteBuf buf = allocate(pool, data.length);
            buf.writeBytes(data);
            return buf;
        } else if (Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM.equals(contentType)) {
            // must set buffer capacity, otherwise Kryo will grow it internally and mess up the pooling
            ByteBuf buf = allocate(pool, ServiceClient.MAX_BINARY_SERIALIZED_BODY_LIMIT);
            KryoSerializers.serializeAsDocument(body, buf);
            op.setContentLength(buf.writerIndex());
            return buf;
        }

        if (contentType == null) {
            op.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
        }
        ByteBuf buf = allocate(pool);
        Utils.toJson(body, buf);
        op.setContentLength(buf.writerIndex());
        return buf;
    }

    public static void encodeAndTransferLinkedStateToBody(Operation source, Operation target, boolean useBinary) {
        if (useBinary && source.getAction() != Action.POST) {
            try {
                ByteBuf buf = encodeBody(source, source.getLinkedState(),
                        Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM, false);
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                source.linkSerializedState(data);
                buf.release();
            } catch (Throwable e2) {
                Utils.logWarning("Failure binary serializing, will fallback to JSON: %s",
                        Utils.toString(e2));
            }
        }

        if (!source.hasLinkedSerializedState()) {
            target.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
            target.setBodyNoCloning(Utils.toJson(source.getLinkedState()));
        } else {
            target.setContentType(Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM);
            target.setBodyNoCloning(source.getLinkedSerializedState());
        }
    }
}
