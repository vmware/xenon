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

import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.VersionFieldSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

public final class KryoSerializers {

    public static class KryoForObjectThreadLocal extends ThreadLocal<Kryo> {
        @Override
        protected Kryo initialValue() {
            return KryoSerializers.create(true);
        }
    }

    public static class KryoForDocumentThreadLocal extends ThreadLocal<Kryo> {
        @Override
        protected Kryo initialValue() {
            return KryoSerializers.create(false);
        }
    }

    public static class KryoForPayloadThreadLocal extends ThreadLocal<Kryo> {
        @Override
        protected Kryo initialValue() {
            return KryoSerializers.create(false, true);
        }
    }

    private KryoSerializers() {
    }

    public static Kryo create(boolean isObjectSerializer) {
        return create(isObjectSerializer, false);
    }

    public static Kryo create(boolean isObjectSerializer, boolean isPayloadSerializer) {
        Kryo k = new Kryo();
        // handle classes with missing default constructors
        k.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        if (isPayloadSerializer) {
            // small overhead, does not support addition/removal of fields
            k.setDefaultSerializer(FieldSerializer.class);
        } else {
            // supports addition of fields if the @since annotation is used
            k.setDefaultSerializer(VersionFieldSerializer.class);
        }
        // Custom serializers for Java 8 date/time
        k.addDefaultSerializer(ZonedDateTime.class, ZonedDateTimeSerializer.INSTANCE);
        k.addDefaultSerializer(Instant.class, InstantSerializer.INSTANCE);
        k.addDefaultSerializer(ZoneId.class, ZoneIdSerializer.INSTANCE);
        // Add non-cloning serializers for all immutable types bellow
        k.addDefaultSerializer(UUID.class, UUIDSerializer.INSTANCE);
        k.addDefaultSerializer(URI.class, URISerializer.INSTANCE);

        if (!isObjectSerializer) {
            // For performance reasons, and to avoid memory use, assume documents do not
            // require object graph serialization with duplicate or recursive references
            k.setReferences(false);
        } else {
            // To avoid monotonic increase of memory use, due to reference tracking, we must
            // reset after each use.
            k.setAutoReset(true);
        }
        return k;
    }
}
