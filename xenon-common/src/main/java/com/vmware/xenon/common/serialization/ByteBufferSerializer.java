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

package com.vmware.xenon.common.serialization;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final class ByteBufferSerializer extends Serializer<ByteBuffer> {
    public static final Serializer<ByteBuffer> INSTANCE = new ByteBufferSerializer();

    @Override
    public void write(Kryo kryo, Output output, ByteBuffer object) {
        int count = object.limit();
        byte[] array = object.array();
        output.writeInt(count);
        output.writeBytes(array, 0, count);
    }

    @Override
    public ByteBuffer read(Kryo kryo, Input input, Class<ByteBuffer> type) {
        int length = input.readInt();
        return ByteBuffer.wrap(input.readBytes(length));
    }
}
