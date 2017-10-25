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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * GSON {@link JsonSerializer}/{@link JsonDeserializer} for representing {@link Collection}s
 * of  objects keyed by strings, whereby the objects are themselves serialized as JSON objects.
 */
public enum ObjectCollectionTypeConverter
        implements JsonSerializer<Collection<Object>>, JsonDeserializer<Collection<Object>> {
    INSTANCE;

    public static final Type TYPE_LIST = TypeTokens.LIST_OF_OBJECTS;
    public static final Type TYPE_SET = TypeTokens.SET_OF_OBJECTS;
    public static final Type TYPE_COLLECTION = TypeTokens.COLLECTION_OF_OBJECTS;

    @Override
    public JsonElement serialize(Collection<Object> set, Type type,
            JsonSerializationContext context) {
        JsonArray setObject = new JsonArray();
        for (Object e : set) {
            if (e == null) {
                setObject.add(JsonNull.INSTANCE);
            } else if (e instanceof JsonElement) {
                setObject.add((JsonElement) e);
            } else {
                setObject.add(context.serialize(e));
            }
        }
        return setObject;
    }

    @Override
    public Collection<Object> deserialize(JsonElement json, Type type,
            JsonDeserializationContext context)
            throws JsonParseException {

        if (!json.isJsonArray()) {
            throw new JsonParseException("Expecting a json array object but found: " + json);
        }

        Collection<Object> result;
        if (TYPE_SET.equals(type)) {
            result = new HashSet<>();
        } else if (TYPE_LIST.equals(type) || TYPE_COLLECTION.equals(type)) {
            result = new LinkedList<>();
        } else {
            throw new JsonParseException("Unexpected target type: " + type);
        }

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
