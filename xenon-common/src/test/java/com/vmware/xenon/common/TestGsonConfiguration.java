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

package com.vmware.xenon.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.Base64;
import java.util.UUID;
import java.util.function.BiFunction;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.serialization.JsonMapper;

public class TestGsonConfiguration {

    private static final String BORING_JSON_DOC_BITS = ",\"documentVersion\":0"
            + ",\"documentUpdateTimeMicros\":0"
            + ",\"documentExpirationTimeMicros\":0";

    @BeforeClass
    public static void setupClass() {
        configureForDocument1();
        configureForDocument2();
    }

    @Test
    public void testCustomConfigurationForClass() {
        SomeDocument1 doc = new SomeDocument1();
        doc.value = new SomeComplexObject("fred", "barney");

        String json = Utils.toJson(doc);

        JsonElement expected = readJson("{ \"value\": \"fred|barney\" "
                + BORING_JSON_DOC_BITS + "}");
        assertEquals(expected, readJson(json));

        SomeDocument1 docBack = Utils.fromJson(json, SomeDocument1.class);
        assertComplexObjectEquals(doc.value, docBack.value);
    }

    @Test
    public void testCustomConfigurationParaemeterizedType() {
        SomeDocument2<SomeComplexObject> doc = new SomeDocument2<SomeComplexObject>();
        doc.value = new SomeComplexObject("fred", "barney");

        String json = Utils.toJson(doc);

        JsonElement expected = readJson("{ \"value\": \"barney&fred\" "
                + BORING_JSON_DOC_BITS + "}");
        assertEquals(expected, readJson(json));

        Type someDocType2 = new TypeToken<SomeDocument2<SomeComplexObject>>() {}.getType();

        SomeDocument2<SomeComplexObject> docBack = Utils.fromJson(json, someDocType2);
        assertComplexObjectEquals(doc.value, docBack.value);
    }

    @Test
    public void testDefaultConfiguration() {
        SomeDocument3 doc = new SomeDocument3();
        doc.value = new SomeComplexObject("fred", "barney");

        String json = Utils.toJson(doc);

        JsonElement expected = readJson("{ \"value\": { "
                + "\"a\": \"fred\", "
                + "\"b\": \"barney\" "
                + "} "
                + BORING_JSON_DOC_BITS
                + "}");
        assertEquals(expected, readJson(json));

        SomeDocument3 docBack = Utils.fromJson(json, SomeDocument3.class);
        assertComplexObjectEquals(doc.value, docBack.value);
    }

    @Test
    public void testPrettyPrinting() {
        SomeDocument1 doc = new SomeDocument1();
        doc.value = new SomeComplexObject("fred", "barney");

        String compact = Utils.toJson(doc);
        String pretty = Utils.toJsonHtml(doc);

        assertTrue(pretty.length() > compact.length());

        JsonElement prettyParsed = readJson(pretty);
        JsonElement compactParsed = readJson(compact);

        assertEquals(compactParsed, prettyParsed);
    }

    @Test
    public void testToJsonWithFieldHiding() {
        // Testing without pretty print
        AnnotatedDoc doc = new AnnotatedDoc();
        doc.value = new SomeComplexObject("fred", "barney");
        doc.sensitiveUsageOption = "sensitive information";
        doc.sensitivePropertyOptions = "sensitive properties";

        String compactRedacted = Utils.toJson(true, false, doc);

        JsonElement expectedRedacted = readJson("{ \"value\": { "
                + "\"a\": \"fred\", "
                + "\"b\": \"barney\" "
                + "} "
                + BORING_JSON_DOC_BITS
                + "}");
        assertEquals(expectedRedacted, readJson(compactRedacted));

        // Testing with pretty print
        String prettyRedacted = Utils.toJson(true, true, doc);

        assertTrue(prettyRedacted.length() > compactRedacted.length());

        JsonElement compactRedactedParsed = readJson(compactRedacted);
        JsonElement prettyRedactedParsed = readJson(prettyRedacted);

        assertEquals(compactRedactedParsed, prettyRedactedParsed);
    }

    private static void assertComplexObjectEquals(
            SomeComplexObject expected,
            SomeComplexObject actual) {
        assertEquals(expected.a, actual.a);
        assertEquals(expected.b, actual.b);
    }

    private static JsonElement readJson(String json) {
        return new JsonParser().parse(json);
    }

    private static void configureForDocument1() {
        Utils.registerCustomJsonMapper(SomeDocument1.class,
                new JsonMapper((b) -> b.registerTypeAdapter(
                        SomeComplexObject.class,
                        SomeBizarreObjectConverter1.INSTANCE)
                ));
    }

    private static void configureForDocument2() {
        Utils.registerCustomJsonMapper(SomeDocument2.class,
                new JsonMapper((b) -> b.registerTypeAdapter(
                        SomeComplexObject.class,
                        SomeBizarreObjectConverter2.INSTANCE)
                ));
    }

    @SuppressWarnings("unchecked")
    public static <T> void assertSerializationPreservesObjects(T instance,
            BiFunction<T, T, Void> comp) {
        String json = Utils.toJson(instance);
        T deserialized = (T) Utils.fromJson(json, instance.getClass());
        comp.apply(instance, deserialized);
    }

    private static final class CustomTypes {
        UUID uuid = UUID.randomUUID();
        URI uri = URI.create("/test/" + this.uuid);
    }

    @Test
    public void testCustomTypesSerialization() throws Exception {
        CustomTypes instance = new CustomTypes();

        assertSerializationPreservesObjects(instance, (original, parsed) -> {
            assertEquals(original.uri, parsed.uri);
            assertEquals(original.uuid, parsed.uuid);
            return null;
        });
    }

    private static class BinaryHolder {
        private byte[] picture;
    }

    @Test
    public void testBinarySerialization() throws Exception {
        BinaryHolder instance = new BinaryHolder();
        instance.picture = "ssdfgsdgsdg".getBytes();

        assertSerializationPreservesObjects(instance, (original, parsed) -> {
            assertArrayEquals(original.picture, parsed.picture);
            return null;
        });
    }

    @Test
    public void testBinaryEncodedToBase64() throws Exception {
        BinaryHolder instance = new BinaryHolder();
        instance.picture = "ssdfgsdgsdg".getBytes();

        String base64encoded = Base64.getEncoder().encodeToString(instance.picture);
        String json = Utils.toJson(instance);
        assertTrue(json.contains(base64encoded));
    }


    public static class AnnotatedDoc extends ServiceDocument {
        public SomeComplexObject value;

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.SENSITIVE)
        public String sensitiveUsageOption;

        @PropertyOptions(usage = {ServiceDocumentDescription.PropertyUsageOption.SENSITIVE})
        public String sensitivePropertyOptions;
    }

    private static class SomeDocument1 extends ServiceDocument {

        public SomeComplexObject value;

    }

    private static class SomeDocument2<T> extends ServiceDocument {

        public T value;

    }

    private static class SomeDocument3 extends ServiceDocument {

        public SomeComplexObject value;

    }

    private static class SomeComplexObject {

        public String a;
        public String b;

        SomeComplexObject() {

        }

        SomeComplexObject(String a, String b) {
            this.a = a;
            this.b = b;
        }

    }

    private enum SomeBizarreObjectConverter1
            implements JsonSerializer<SomeComplexObject>, JsonDeserializer<SomeComplexObject> {
        INSTANCE;

        @Override
        public JsonElement serialize(SomeComplexObject src, Type typeOfSrc,
                JsonSerializationContext context) {
            return new JsonPrimitive(src.a + "|" + src.b);
        }

        @Override
        public SomeComplexObject deserialize(JsonElement json, Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {
            String[] splat = json.getAsString().split("\\|");

            SomeComplexObject res = new SomeComplexObject();
            res.a = splat[0];
            res.b = splat[1];
            return res;
        }

    }

    private enum SomeBizarreObjectConverter2
            implements JsonSerializer<SomeComplexObject>, JsonDeserializer<SomeComplexObject> {
        INSTANCE;

        @Override
        public JsonElement serialize(SomeComplexObject src, Type typeOfSrc,
                JsonSerializationContext context) {
            return new JsonPrimitive(src.b + "&" + src.a);
        }

        @Override
        public SomeComplexObject deserialize(JsonElement json, Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {
            String[] splat = json.getAsString().split("&");

            SomeComplexObject res = new SomeComplexObject();
            res.a = splat[1];
            res.b = splat[0];
            return res;
        }

    }

}
