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

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Date;
import java.util.function.Consumer;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.gson.internal.Streams;
import com.google.gson.internal.bind.JsonTreeWriter;
import com.google.gson.stream.JsonWriter;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.Utils;

/**
 * A helper that serializes/deserializes service documents to/from JSON. The implementation uses a
 * pair of {@link Gson} instances: one for compact printing; the other for pretty-printed,
 * HTML-friendly output.
 */
public class JsonMapper {

    public static final String PROPERTY_JSON_SUPPRESS_GSON_SERIALIZATION_ERRORS
            = Utils.PROPERTY_NAME_PREFIX + "json.suppressGsonSerializationErrors";

    private static boolean JSON_SUPPRESS_GSON_SERIALIZATION_ERRORS = false;

    static {
        String v = System.getProperty(PROPERTY_JSON_SUPPRESS_GSON_SERIALIZATION_ERRORS);
        if (v != null) {
            JSON_SUPPRESS_GSON_SERIALIZATION_ERRORS = Boolean.valueOf(v);
        }
    }

    private static final int MAX_SERIALIZATION_ATTEMPTS = 100;
    private static final String JSON_INDENT = "  ";

    private final Gson compact;
    private Gson hashing;
    private final Gson compactSensitive;
    private boolean jsonSuppressGsonSerializationErrors = JSON_SUPPRESS_GSON_SERIALIZATION_ERRORS;

    /**
     * Instantiates a mapper with default GSON configurations.
     */
    public JsonMapper() {
        this(createDefaultGson(true, false),
                createDefaultGson(true, true));
    }

    /**
     * Instantiates a mapper with allowing custom GSON configurations. These configurations can be
     * made by supplying a callback which can manipulate the {@link GsonBuilder} while the GSON
     * instances are under construction. This callback is invoked twice, once each for the pretty
     * and compact instances.
     */
    public JsonMapper(Consumer<GsonBuilder> gsonConfigCallback) {
        this(createCustomGson(true, false, gsonConfigCallback),
                createCustomGson(true, true, gsonConfigCallback));
        this.hashing = createHashingGson(gsonConfigCallback);
    }

    /**
     * Instantiates a mapper with the compact and pretty GSON instances explicitly supplied.
     */
    public JsonMapper(Gson compact, Gson compactSensitive) {
        this.compact = compact;
        this.compactSensitive = compactSensitive;
        this.hashing = createHashingGson(null);
    }

    private Gson createHashingGson(Consumer<GsonBuilder> gsonConfigCallback) {
        GsonBuilder bldr = new GsonBuilder();

        registerCommonGsonTypeAdapters(bldr);

        bldr.disableHtmlEscaping();

        bldr.registerTypeAdapterFactory(new SortedKeysMapViewAdapterFactory());
        if (gsonConfigCallback != null) {
            gsonConfigCallback.accept(bldr);
        }
        return bldr.create();
    }

    /**
     * Use {@link #JsonMapper(Gson, Gson)} instead.
     * The extra parameters are ignored.
     *
     * @param compact
     * @param pretty
     * @param compactSensitive
     * @param prettySensitive
     */
    @Deprecated
    public JsonMapper(Gson compact, Gson pretty, Gson compactSensitive, Gson prettySensitive) {
        this(compact, compactSensitive);
    }

    /**
     * Outputs a JSON representation of the given object in compact JSON.
     */
    public String toJson(Object body) {
        for (int i = 1; ; i++) {
            try {
                return this.compact.toJson(body);
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    public void toJson(Object body, Appendable appendable) {
        for (int i = 1; ; i++) {
            try {
                this.compact.toJson(body, appendable);
                return;
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    /**
     * Convert an object to JsonElement without intermediate string representations.
     * @param body
     * @return
     */
    public JsonElement toJsonElement(Object body) {
        if (body == null) {
            return null;
        }

        for (int i = 1; ; i++) {
            try {
                JsonTreeWriter writer = new JsonTreeWriter();
                this.compact.toJson(body, body.getClass(), writer);
                return writer.get();
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    /**
     * Outputs a JSON representation of the given object in pretty-printed, HTML-friendly JSON.
     */
    public String toJsonHtml(Object body) {
        if (body == null) {
            return this.compact.toJson(null);
        }

        for (int i = 1; ; i++) {
            try {
                StringBuilder appendable = new StringBuilder();
                this.toJsonHtml(body, appendable);
                return appendable.toString();
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    /**
     * Outputs a JSON representation of the given object using useHTMLFormatting to create pretty-printed,
     * HTML-friendly JSON or compact JSON. If hideSensitiveFields is set the JSON will not include fields
     * with the annotation {@link com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption#SENSITIVE}.
     */
    public void toJson(boolean hideSensitiveFields, boolean useHtmlFormatting, Object body, Appendable appendable) {
        for (int i = 1; ; i++) {
            try {
                if (hideSensitiveFields) {
                    if (useHtmlFormatting) {
                        this.compactSensitive.toJson(body, body.getClass(), makePrettyJsonWriter(appendable));
                        return;
                    } else {
                        this.compactSensitive.toJson(body, appendable);
                        return;
                    }
                } else {
                    if (useHtmlFormatting) {
                        this.compact.toJson(body, body.getClass(), makePrettyJsonWriter(appendable));
                        return;
                    } else {
                        this.compact.toJson(body, appendable);
                        return;
                    }
                }
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
        try {
            if (json instanceof JsonElement) {
                return this.compact.fromJson((JsonElement) json, type);
            } else {
                return this.compact.fromJson(json.toString(), type);
            }
        } catch (RuntimeException e) {
            if (this.jsonSuppressGsonSerializationErrors) {
                throw new JsonSyntaxException("JSON body could not be parsed");
            } else {
                throw e;
            }
        }
    }

    public void setJsonSuppressGsonSerializationErrors(boolean suppressErrors) {
        this.jsonSuppressGsonSerializationErrors = suppressErrors;
    }

    private static Gson createDefaultGson(boolean isCompact, boolean isSensitive) {
        return createDefaultGsonBuilder(isCompact, isSensitive).create();
    }

    private static Gson createCustomGson(boolean isCompact, boolean isSensitive,
            Consumer<GsonBuilder> gsonConfigCallback) {
        GsonBuilder bldr = createDefaultGsonBuilder(isCompact, isSensitive);
        gsonConfigCallback.accept(bldr);
        return bldr.create();
    }

    public static GsonBuilder createDefaultGsonBuilder(boolean isCompact, boolean isSensitive) {
        GsonBuilder bldr = new GsonBuilder();

        registerCommonGsonTypeAdapters(bldr);

        if (!isCompact) {
            bldr.setPrettyPrinting();
        }

        bldr.disableHtmlEscaping();

        if (isSensitive) {
            bldr.addSerializationExclusionStrategy(new SensitiveAnnotationExclusionStrategy());
        }

        return bldr;
    }

    private static void registerCommonGsonTypeAdapters(GsonBuilder bldr) {
        bldr.registerTypeAdapter(ObjectMapTypeConverter.TYPE, ObjectMapTypeConverter.INSTANCE);
        bldr.registerTypeAdapter(InstantConverter.TYPE, InstantConverter.INSTANCE);
        bldr.registerTypeAdapter(ZonedDateTimeConverter.TYPE, ZonedDateTimeConverter.INSTANCE);
        bldr.registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter());
        bldr.registerTypeAdapter(RequestRouteConverter.TYPE, RequestRouteConverter.INSTANCE);
        bldr.registerTypeHierarchyAdapter(Path.class, PathConverter.INSTANCE);
        bldr.registerTypeHierarchyAdapter(Date.class, UtcDateTypeAdapter.INSTANCE);
    }

    public void toJsonHtml(Object body, Appendable appendable) {
        if (body == null) {
            this.compact.toJson(null, appendable);
            return;
        }

        for (int i = 1; ; i++) {
            try {
                JsonWriter jsonWriter = makePrettyJsonWriter(appendable);
                this.compact.toJson(body, body.getClass(), jsonWriter);
                return;
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    private JsonWriter makePrettyJsonWriter(Appendable appendable) {
        JsonWriter jsonWriter = new JsonWriter(Streams.writerForAppendable(appendable));
        jsonWriter.setIndent(JSON_INDENT);
        return jsonWriter;
    }

    public long hashJson(Object body, long seed) {
        if (body == null) {
            return seed;
        }

        for (int i = 1; ; i++) {
            try {
                HashingJsonWriter w = new HashingJsonWriter(seed);
                this.hashing.toJson(body, body.getClass(), w);

                return w.getHash();
            } catch (IllegalStateException e) {
                handleIllegalStateException(e, i);
            }
        }
    }

    /**
     * Excludes any field with the SENSITIVE option in PropertyUsageOptions.
     */
    private static class SensitiveAnnotationExclusionStrategy implements ExclusionStrategy {

        @Override
        public boolean shouldSkipField(FieldAttributes fieldAttributes) {
            Collection<Annotation> annotations = fieldAttributes.getAnnotations();

            for (Annotation a : annotations) {
                if (ServiceDocument.UsageOptions.class.equals(a.annotationType())) {
                    ServiceDocument.UsageOptions usageOptions = (ServiceDocument.UsageOptions) a;
                    for (ServiceDocument.UsageOption usageOption : usageOptions.value()) {
                        if (usageOption.option().equals(ServiceDocumentDescription.PropertyUsageOption.SENSITIVE)) {
                            return true;
                        }
                    }
                } else if (ServiceDocument.UsageOption.class.equals(a.annotationType())) {
                    ServiceDocument.UsageOption usageOption = (ServiceDocument.UsageOption) a;
                    if (usageOption.option().equals(ServiceDocumentDescription.PropertyUsageOption.SENSITIVE)) {
                        return true;
                    }
                } else if (ServiceDocument.PropertyOptions.class.equals(a.annotationType())) {
                    ServiceDocument.PropertyOptions propertyOptions = (ServiceDocument.PropertyOptions) a;
                    for (ServiceDocumentDescription.PropertyUsageOption propertyUsageOption : propertyOptions.usage()) {
                        if (propertyUsageOption.equals(ServiceDocumentDescription.PropertyUsageOption.SENSITIVE)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public boolean shouldSkipClass(Class<?> aClass) {
            return false;
        }
    }
}
