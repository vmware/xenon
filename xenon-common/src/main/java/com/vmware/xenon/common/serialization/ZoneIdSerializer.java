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

import java.time.ZoneId;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A Kryo {@link Serializer} for supporting cloning a Java 8 {@link ZoneId}.
 */
public class ZoneIdSerializer extends Serializer<ZoneId> {

    public static final ZoneIdSerializer INSTANCE = new ZoneIdSerializer();

    public ZoneIdSerializer() {
        setAcceptsNull(false);
        setImmutable(true);
    }

    @Override
    public void write(Kryo kryo, Output output, ZoneId object) {
        write(output, object);
    }

    @Override
    public ZoneId read(Kryo kryo, Input input, Class<ZoneId> type) {
        return read(input);
    }

    protected static void write(Output output, ZoneId object) {
        output.writeString(object.getId());
    }

    protected static ZoneId read(Input input) {
        String id = input.readString();
        return ZoneId.of(id);
    }

}
