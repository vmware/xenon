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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * GSON {@link JsonSerializer}/{@link JsonDeserializer} for representing {@link Map}s of objects
 * keyed by strings, whereby the objects are themselves serialized as JSON objects.
 */
public enum ObjectMapTypeConverter
        implements JsonSerializer<Map<String, Object>>, JsonDeserializer<Map<String, Object>> {
    INSTANCE;

    public static final Type TYPE = TypeTokens.MAP_OF_OBJECTS_BY_STRING;

    @Override
    public JsonElement serialize(Map<String, Object> map, Type type,
            JsonSerializationContext context) {
        JsonObject mapObject = new JsonObject();
        for (Entry<String, Object> e : map.entrySet()) {
            Object v = e.getValue();
            if (v == null) {
                mapObject.add(e.getKey(), JsonNull.INSTANCE);
            } else if (v instanceof JsonObject) {
                mapObject.add(e.getKey(), (JsonObject) v);
            } else if (v instanceof String) {
                mapObject.add(e.getKey(), new JsonParser().parse((String) v));
            } else {
                mapObject.add(e.getKey(), context.serialize(v));
            }
        }
        return mapObject;
    }

    @Override
    public Map<String, Object> deserialize(JsonElement json, Type unused,
            JsonDeserializationContext context)
            throws JsonParseException {

        if (!json.isJsonObject()) {
            throw new JsonParseException("Expecting a json Map object but found: " + json);
        }

        Map<String, Object> result = new HashMap<>();
        JsonObject jsonObject = json.getAsJsonObject();
        for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            JsonElement element = entry.getValue();
            if (element.isJsonNull()) {
                result.put(key, null);
            } else if (element.isJsonPrimitive()) {
                JsonPrimitive elem = element.getAsJsonPrimitive();
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
                    throw new RuntimeException("Unexpected value type for json element key:" + key
                            + " value:" + element);
                }
                result.put(key, value);
            } else if (element.isJsonArray()) {
                result.put(key, element.getAsJsonArray().toString());
            } else if (element.isJsonObject()) {
                result.put(key, element.toString());

            } else {
                throw new JsonParseException("The json element is not valid for key:" + key
                        + " value:" + element);
            }
        }
        return result;
    }

}
