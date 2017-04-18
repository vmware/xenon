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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.vmware.xenon.common.FNVHash;

/**
 * Infrastructure use only.
 * Entrypoint to Gson customization/internals.
 */
public final class GsonSerializers {
    private static final JsonMapper JSON = new JsonMapper();

    private static final ConcurrentMap<Class<?>, JsonMapper> CUSTOM_JSON = new ConcurrentSkipListMap<>(
            Comparator.comparingInt((Class<?> c) -> c.hashCode()).<String>thenComparing(Class::getName));

    private GsonSerializers() {

    }

    public static JsonMapper getJsonMapperFor(Object instance) {
        if (instance == null) {
            return JSON;
        }
        return getJsonMapperFor(instance.getClass());
    }

    public static JsonMapper getJsonMapperFor(Class<?> type) {
        if (type.isArray() && type != byte[].class) {
            type = type.getComponentType();
        }
        return CUSTOM_JSON.getOrDefault(type, JSON);
    }

    public static JsonMapper getJsonMapperFor(Type type) {
        if (type instanceof Class) {
            return getJsonMapperFor((Class<?>) type);
        } else if (type instanceof ParameterizedType) {
            Type rawType = ((ParameterizedType) type).getRawType();
            return getJsonMapperFor(rawType);
        } else {
            return JSON;
        }
    }

    public static void registerCustomJsonMapper(Class<?> clazz,
            JsonMapper mapper) {
        CUSTOM_JSON.putIfAbsent(clazz, mapper);
    }

    public static long hashJson(Object body, long hash) {
        if (body instanceof String) {
            return FNVHash.compute((String) body, hash);
        }

        JsonMapper mapper = getJsonMapperFor(body);
        return mapper.hashJson(body, hash);
    }
}
