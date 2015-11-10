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

import java.time.Instant;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A Kryo {@link Serializer} for supporting cloning a Java 8 {@link Instant}.
 */
public class InstantSerializer extends Serializer<Instant> {

    public static final InstantSerializer INSTANCE = new InstantSerializer();

    public InstantSerializer() {
        setAcceptsNull(false);
        setImmutable(true);
    }

    @Override
    public void write(Kryo kryo, Output output, Instant object) {
        write(output, object);
    }

    @Override
    public Instant read(Kryo kryo, Input input, Class<Instant> type) {
        return read(input);
    }

    protected static void write(Output output, Instant object) {
        output.writeLong(object.getEpochSecond());
        output.writeInt(object.getNano());
    }

    protected static Instant read(Input input) {
        long epochSecond = input.readLong();
        int nano = input.readInt();
        return Instant.ofEpochSecond(epochSecond, nano);
    }

}
