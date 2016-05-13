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
import java.util.function.Consumer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import com.vmware.xenon.common.Utils;

/**
 * A helper that serializes/deserializes service documents to/from JSON. The implementation uses a
 * pair of {@link Gson} instances: one for compact printing; the other for pretty-printed,
 * HTML-friendly output.
 */
public class JsonMapper {

    private static final int MAX_SERIALIZATION_ATTEMPTS = 100;

    private final Gson compact;
    private final Gson pretty;

    /**
     * Instantiates a mapper with default GSON configurations.
     */
    public JsonMapper() {
        this(createDefaultGson(true),
                createDefaultGson(false));
    }

    /**
     * Instantiates a mapper with allowing custom GSON configurations. These configurations can be
     * made by supplying a callback which can manipulate the {@link GsonBuilder} while the GSON
     * instances are under construction. This callback is invoked twice, once each for the pretty
     * and compact instances.
     */
    public JsonMapper(Consumer<GsonBuilder> gsonConfigCallback) {
        this(createCustomGson(true, gsonConfigCallback),
                createCustomGson(false, gsonConfigCallback));
    }

    /**
     * Instantiates a mapper with the compact and pretty GSON instances explicitly supplied.
     */
    public JsonMapper(Gson compact, Gson pretty) {
        this.compact = compact;
        this.pretty = pretty;
    }

    /**
     * Outputs a JSON representation of the given object in compact JSON.
     */
    public String toJson(Object body) {
        for (int i = 1;; i++) {
            try {
                return this.compact.toJson(body);
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    public void toJson(Object body, Appendable appendable) {
        for (int i = 1;; i++) {
            try {
                this.compact.toJson(body, appendable);
                return;
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    /**
     * Outputs a JSON representation of the given object in pretty-printed, HTML-friendly JSON.
     */
    public String toJsonHtml(Object body) {
        for (int i = 1;; i++) {
            try {
                return this.pretty.toJson(body);
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    private void handleIllegalStateException(IllegalStateException e, int i) {
        if (e.getMessage() == null) {
            Utils.logWarning("Failure serializing body because of GSON race (attempt %d)", i);
            if (i >= MAX_SERIALIZATION_ATTEMPTS) {
                throw e;
            }

            // This error may happen when two threads try to serialize a recursive
            // type for the very first time concurrently. Type caching logic in GSON
            // doesn't deal well with recursive types being generated concurrently.
            // Also see: https://github.com/google/gson/issues/764
            try {
                Thread.sleep(0, 1000 * i);
            } catch (InterruptedException ignored) {
            }
            return;
        }

        throw e;
    }

    /**
     * Deserializes the given JSON to the target {@link Class}.
     */
    public <T> T fromJson(Object json, Class<T> clazz) {
        if (clazz.isInstance(json)) {
            return clazz.cast(json);
        } else {
            return fromJson(json, (Type) clazz);
        }
    }

    /**
     * Deserializes the given JSON to the target {@link Type}.
     */
    public <T> T fromJson(Object json, Type type) {
        if (json instanceof JsonElement) {
            return this.compact.fromJson((JsonElement) json, type);
        } else {
            return this.compact.fromJson(json.toString(), type);
        }
    }

    private static Gson createDefaultGson(boolean isCompact) {
        return createDefaultGsonBuilder(isCompact).create();
    }

    private static Gson createCustomGson(boolean isCompact,
            Consumer<GsonBuilder> gsonConfigCallback) {
        GsonBuilder bldr = createDefaultGsonBuilder(isCompact);
        gsonConfigCallback.accept(bldr);
        return bldr.create();
    }

    public static GsonBuilder createDefaultGsonBuilder(boolean isCompact) {
        GsonBuilder bldr = new GsonBuilder();

        registerCommonGsonTypeAdapters(bldr);

        if (isCompact) {
            bldr.disableHtmlEscaping();
        } else {
            bldr.setPrettyPrinting();
        }

        return bldr;
    }

    private static void registerCommonGsonTypeAdapters(GsonBuilder bldr) {
        bldr.registerTypeAdapter(ObjectMapTypeConverter.TYPE, ObjectMapTypeConverter.INSTANCE);
        bldr.registerTypeAdapter(InstantConverter.TYPE, InstantConverter.INSTANCE);
        bldr.registerTypeAdapter(ZonedDateTimeConverter.TYPE, ZonedDateTimeConverter.INSTANCE);
        bldr.registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter());
        bldr.registerTypeAdapter(RequestRouteConverter.TYPE, RequestRouteConverter.INSTANCE);
    }

    public void toJsonHtml(Object body, Appendable appendable) {
        for (int i = 1;; i++) {
            try {
                this.pretty.toJson(body, appendable);
                return;
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }
}
