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

package com.vmware.dcp.common.serialization;

import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Based on
 * https://github.com/Cascading/meat-locker/blob/master/src/jvm/com/twitter/meatlocker/kryo/UUIDSerializer.java
 */
public class UUIDSerializer extends Serializer<UUID> {

    public static final UUIDSerializer INSTANCE = new UUIDSerializer();

    public UUIDSerializer() {
        setImmutable(true);
        setAcceptsNull(false);
    }

    @Override
    public void write(Kryo kryo, Output output, UUID uuid) {
        output.writeLong(uuid.getMostSignificantBits(), false);
        output.writeLong(uuid.getLeastSignificantBits(), false);
    }

    @Override
    public UUID read(Kryo kryo, Input input, Class<UUID> uuidClass) {
        return new UUID(input.readLong(false), input.readLong(false));
    }
}
