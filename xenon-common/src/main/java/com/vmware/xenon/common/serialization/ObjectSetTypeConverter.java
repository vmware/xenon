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

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * GSON {@link JsonSerializer}/{@link JsonDeserializer} for representing {@link Set}s of objects
 * keyed by strings, whereby the objects are themselves serialized as JSON objects.
 */
public enum ObjectSetTypeConverter
        implements JsonSerializer<Set<Object>>, JsonDeserializer<Set<Object>> {
    INSTANCE;

    public static final Type TYPE = TypeTokens.SET_OF_OBJECTS;

    @Override
    public JsonElement serialize(Set<Object> set, Type type,
            JsonSerializationContext context) {
        JsonArray setObject = new JsonArray();
        for (Object e : set) {
            if (e == null) {
                setObject.add(JsonNull.INSTANCE);
            } else if (e instanceof JsonElement) {
                setObject.add((JsonElement) e);
            } else if (e instanceof String) {
                setObject.add(new JsonParser().parse((String) e));
            } else {
                setObject.add(context.serialize(e));
            }
        }
        return setObject;
    }

    @Override
    public Set<Object> deserialize(JsonElement json, Type unused,
            JsonDeserializationContext context)
            throws JsonParseException {

        if (!json.isJsonArray()) {
            throw new JsonParseException("Expecting a json array object but found: " + json);
        }

        Set<Object> result = new HashSet<>();
        JsonArray jsonArray = json.getAsJsonArray();
        for (JsonElement entry : jsonArray) {
            if (entry.isJsonNull()) {
                result.add(null);
            } else if (entry.isJsonPrimitive()) {
                JsonPrimitive elem = entry.getAsJsonPrimitive();
                Object value = null;
                if (elem.isBoolean()) {
                    value = elem.getAsBoolean();
                } else if (elem.isString()) {
                    value = elem.getAsString();
                } else if (elem.isNumber()) {
                    // We don't know if this is an integer, long, float or double...
                    BigDecimal num = elem.getAsBigDecimal();
                    try {
                        value = num.longValueExact();
                    } catch (ArithmeticException e) {
                        value = num.doubleValue();
                    }
                } else {
                    throw new RuntimeException("Unexpected value type for json element:" + elem);
                }
                result.add(value);
            } else {
                // keep JsonElement to prevent stringified json issues
                result.add(entry);
            }
        }
        return result;
    }

}
