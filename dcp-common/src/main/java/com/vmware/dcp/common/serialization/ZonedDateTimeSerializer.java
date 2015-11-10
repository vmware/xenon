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
import java.time.ZoneId;
import java.time.ZonedDateTime;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A Kryo {@link Serializer} for supporting cloning a Java 8 {@link ZonedDateTime}.
 */
public class ZonedDateTimeSerializer extends Serializer<ZonedDateTime> {

    public static final ZonedDateTimeSerializer INSTANCE = new ZonedDateTimeSerializer();

    private ZonedDateTimeSerializer() {
        setAcceptsNull(false);
        setImmutable(true);
    }

    @Override
    public void write(Kryo kryo, Output output, ZonedDateTime object) {
        InstantSerializer.write(output, object.toInstant());
        ZoneIdSerializer.write(output, object.getZone());
    }

    @Override
    public ZonedDateTime read(Kryo kryo, Input input, Class<ZonedDateTime> type) {
        Instant instant = InstantSerializer.read(input);
        ZoneId zone = ZoneIdSerializer.read(input);
        return ZonedDateTime.ofInstant(instant, zone);
    }

}
