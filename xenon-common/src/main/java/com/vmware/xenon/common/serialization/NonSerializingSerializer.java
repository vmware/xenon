/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NonSerializingSerializer extends com.esotericsoftware.kryo.Serializer<Object> {
    private static final NonSerializingSerializer INSTANCE = new NonSerializingSerializer();

    // TODO add all classes from the paper: https://www.github.com/mbechler/marshalsec/blob/master/marshalsec.pdf?raw=true
    // add more jvm backdoor classes here
    private static final Set<Class<?>> BLACKLISTED = new HashSet<>(Arrays.asList(
            javax.naming.spi.ObjectFactory.class));

    @SuppressWarnings("unchecked")
    public static <P> Serializer<P> getInstance() {
        return (Serializer<P>) INSTANCE;
    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        // do nothing
    }

    @Override
    public Object read(Kryo kryo, Input input, Class<Object> type) {
        // return null, don't overreact with an exception
        return null;
    }

    public static void apply(Kryo k) {
        for (Class<?> cls : NonSerializingSerializer.BLACKLISTED) {
            k.addDefaultSerializer(cls, NonSerializingSerializer.getInstance());
        }
    }
}
