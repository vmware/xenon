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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost.Arguments;
import com.vmware.xenon.common.TestGsonConfiguration;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;

public class TestJsonMapper {
    private static final int NUM_THREADS = 2;

    // An error may happen when two threads try to serialize a recursive
    // type for the very first time concurrently. Type caching logic in GSON
    // doesn't deal well with recursive types being generated concurrently.
    // Also see: https://github.com/google/gson/issues/764
    @Test
    public void testConcurrency() throws InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        Query q = Builder.create()
                .addFieldClause(
                        "kind",
                        "foo")
                .addFieldClause(
                        "name",
                        "jane")
                .build();

        for (int i = 0; i < 1000; i++) {
            final CountDownLatch start = new CountDownLatch(1);
            final TestContext finish = new TestContext(NUM_THREADS, Duration.ofSeconds(30));
            final JsonMapper mapper = new JsonMapper();

            for (int j = 0; j < NUM_THREADS; j++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        mapper.toJson(q);
                        finish.complete();
                    } catch (Throwable t) {
                        finish.fail(t);
                    }
                });
            }

            start.countDown();
            finish.await();
        }
    }

    @Test
    public void testObjectMapValue() {
        JsonMapTestObject srcMap = new JsonMapTestObject();
        srcMap.map = new HashMap<>();

        srcMap.map.put("str", "/foo/bar");
        srcMap.map.put("long", Long.MAX_VALUE);
        srcMap.map.put("double", new Double(3.14));
        srcMap.map.put("boolean", Boolean.TRUE);
        srcMap.map.put("object", new ComplexObject("a", "b"));

        String json = Utils.toJson(srcMap);

        JsonMapTestObject dstMap = Utils.fromJson(json, JsonMapTestObject.class);
        // serialize once again to make sure our function is really idempotent
        dstMap = Utils.fromJson(Utils.toJson(dstMap), JsonMapTestObject.class);

        Assert.assertEquals(srcMap.map.size(), dstMap.map.size());
        for (Object entry : dstMap.map.values()) {
            if (entry instanceof String) {
                Assert.assertEquals("/foo/bar", entry);
            } else if (entry instanceof Long) {
                Assert.assertEquals(Long.MAX_VALUE, entry);
            } else if (entry instanceof Double) {
                Assert.assertEquals(new Double(3.14), entry);
            } else if (entry instanceof Boolean) {
                Assert.assertEquals(Boolean.TRUE, entry);
            } else if (entry instanceof JsonElement) {
                JsonElement expected = Utils.fromJson(Utils.toJson(new ComplexObject("a", "b")),
                        JsonElement.class);
                Assert.assertEquals(expected, entry);
            } else {
                Assert.fail("unexpected entry: " + entry);
            }
        }
    }

    @Test
    public void testObjectCollectionValue() {
        JsonColTestObject srcCol = new JsonColTestObject();
        srcCol.collection = new HashSet<>();

        srcCol.collection.add("/foo/bar");
        srcCol.collection.add(Long.MAX_VALUE);
        srcCol.collection.add(new Double(3.14));
        srcCol.collection.add(Boolean.TRUE);
        srcCol.collection.add(new ComplexObject("a", "b"));

        String json = Utils.toJson(srcCol);

        JsonColTestObject dstCol = Utils.fromJson(json, JsonColTestObject.class);
        // serialize once again to make sure our function is really idempotent
        dstCol = Utils.fromJson(Utils.toJson(dstCol), JsonColTestObject.class);

        Assert.assertEquals(srcCol.collection.size(), dstCol.collection.size());
        for (Object entry : dstCol.collection) {
            if (entry instanceof String) {
                Assert.assertEquals("/foo/bar", entry);
            } else if (entry instanceof Long) {
                Assert.assertEquals(Long.MAX_VALUE, entry);
            } else if (entry instanceof Double) {
                Assert.assertEquals(new Double(3.14), entry);
            } else if (entry instanceof Boolean) {
                Assert.assertEquals(Boolean.TRUE, entry);
            } else if (entry instanceof JsonElement) {
                JsonElement expected = Utils.fromJson(Utils.toJson(new ComplexObject("a", "b")),
                        JsonElement.class);
                Assert.assertEquals(expected, entry);
            } else {
                Assert.fail("unexpected entry: " + entry);
            }
        }
    }

    @Test
    public void testObjectSetValue() {
        JsonSetTestObject srcSet = new JsonSetTestObject();
        srcSet.set = new HashSet<>();

        srcSet.set.add("/foo/bar");
        srcSet.set.add(Long.MAX_VALUE);
        srcSet.set.add(new Double(3.14));
        srcSet.set.add(Boolean.TRUE);
        srcSet.set.add(new ComplexObject("a", "b"));

        String json = Utils.toJson(srcSet);

        JsonSetTestObject dstSet = Utils.fromJson(json, JsonSetTestObject.class);
        // serialize once again to make sure our function is really idempotent
        dstSet = Utils.fromJson(Utils.toJson(dstSet), JsonSetTestObject.class);

        Assert.assertEquals(srcSet.set.size(), dstSet.set.size());
        for (Object entry : dstSet.set) {
            if (entry instanceof String) {
                Assert.assertEquals("/foo/bar", entry);
            } else if (entry instanceof Long) {
                Assert.assertEquals(Long.MAX_VALUE, entry);
            } else if (entry instanceof Double) {
                Assert.assertEquals(new Double(3.14), entry);
            } else if (entry instanceof Boolean) {
                Assert.assertEquals(Boolean.TRUE, entry);
            } else if (entry instanceof JsonElement) {
                JsonElement expected = Utils.fromJson(Utils.toJson(new ComplexObject("a", "b")),
                        JsonElement.class);
                Assert.assertEquals(expected, entry);
            } else {
                Assert.fail("unexpected entry: " + entry);
            }
        }
    }

    @Test
    public void testObjectListValue() {
        JsonListTestObject srcList = new JsonListTestObject();
        srcList.list = new ArrayList<>();

        srcList.list.add("/foo/bar");
        srcList.list.add(Long.MAX_VALUE);
        srcList.list.add(new Double(3.14));
        srcList.list.add(Boolean.TRUE);
        srcList.list.add(new ComplexObject("a", "b"));

        String json = Utils.toJson(srcList);

        JsonListTestObject dstList = Utils.fromJson(json, JsonListTestObject.class);
        // serialize once again to make sure our function is really idempotent
        dstList = Utils.fromJson(Utils.toJson(dstList), JsonListTestObject.class);

        Assert.assertEquals(srcList.list.size(), dstList.list.size());
        for (Object entry : dstList.list) {
            if (entry instanceof String) {
                Assert.assertEquals("/foo/bar", entry);
            } else if (entry instanceof Long) {
                Assert.assertEquals(Long.MAX_VALUE, entry);
            } else if (entry instanceof Double) {
                Assert.assertEquals(new Double(3.14), entry);
            } else if (entry instanceof Boolean) {
                Assert.assertEquals(Boolean.TRUE, entry);
            } else if (entry instanceof JsonElement) {
                JsonElement expected = Utils.fromJson(Utils.toJson(new ComplexObject("a", "b")),
                        JsonElement.class);
                Assert.assertEquals(expected, entry);
            } else {
                Assert.fail("unexpected entry: " + entry);
            }
        }
    }

    public static class JsonMapTestObject {
        public Map<String, Object> map;
    }

    public static class JsonListTestObject {
        public List<Object> list;
    }

    public static class JsonSetTestObject {
        public Set<Object> set;
    }

    public static class JsonColTestObject {
        public Collection<Object> collection;
    }

    public static class ComplexObject {
        public String prop1;
        public String prop2;

        public ComplexObject() {

        }

        public ComplexObject(String prop1, String prop2) {
            super();
            this.prop1 = prop1;
            this.prop2 = prop2;
        }
    }

    @Ignore("https://www.pivotaltracker.com/story/show/151532080 Fail on windows")
    @Test
    public void testPathJsonSerialization() {
        Path p = Paths.get("test");

        String jsonRepr = Utils.toJson(p);
        assertEquals("\"" + p.toAbsolutePath().toAbsolutePath() + "\"", jsonRepr);

        Arguments arguments = new Arguments();
        Logger.getAnonymousLogger().info(Utils.toJsonHtml(arguments));
    }

    @Test
    public void testJsonOptions() {
        TestGsonConfiguration.AnnotatedDoc testDoc = new TestGsonConfiguration.AnnotatedDoc();
        testDoc.value = new TestGsonConfiguration.SomeComplexObject("complexA", "complexB");
        testDoc.sensitivePropertyOptions = "sensitive data1";
        testDoc.sensitiveUsageOption = "sensitive data2";
        Pattern containsWhitespacePattern = Pattern.compile("\\s");

        String jsonIncludeSensitiveAndBuiltInPrettyPrinted = Utils.toJson(EnumSet.noneOf(JsonMapper.JsonOptions.class), testDoc);
        assertThat(jsonIncludeSensitiveAndBuiltInPrettyPrinted, containsString("complexA"));
        assertThat(jsonIncludeSensitiveAndBuiltInPrettyPrinted, containsString("sensitive"));
        assertThat(jsonIncludeSensitiveAndBuiltInPrettyPrinted, containsString(ServiceDocument.FIELD_NAME_VERSION));
        assertTrue("pretty-printed JSON should have whitespaces",
                containsWhitespacePattern.matcher(jsonIncludeSensitiveAndBuiltInPrettyPrinted).find());

        String jsonExcludeSensitiveIncludesBuiltIn = Utils.toJson(EnumSet.of(JsonMapper.JsonOptions.EXCLUDE_SENSITIVE), testDoc);
        assertThat(jsonExcludeSensitiveIncludesBuiltIn, containsString("complexA"));
        assertThat(jsonExcludeSensitiveIncludesBuiltIn, not(containsString("sensitive")));
        assertThat(jsonExcludeSensitiveIncludesBuiltIn, containsString(ServiceDocument.FIELD_NAME_VERSION));

        String jsonIncludeSensitiveExcludeBuiltIn = Utils.toJson(EnumSet.of(JsonMapper.JsonOptions.EXCLUDE_BUILTIN), testDoc);
        assertThat(jsonIncludeSensitiveExcludeBuiltIn, containsString("complexA"));
        assertThat(jsonIncludeSensitiveExcludeBuiltIn, containsString("sensitive"));
        assertThat(jsonIncludeSensitiveExcludeBuiltIn, not(containsString(ServiceDocument.FIELD_NAME_VERSION)));

        String jsonExcludeSensitiveAndBuiltInCompact = Utils.toJson(
                EnumSet.of(JsonMapper.JsonOptions.EXCLUDE_SENSITIVE, JsonMapper.JsonOptions.EXCLUDE_BUILTIN, JsonMapper.JsonOptions.COMPACT),
                testDoc);
        assertThat(jsonExcludeSensitiveAndBuiltInCompact, containsString("complexA"));
        assertThat(jsonExcludeSensitiveAndBuiltInCompact, not(containsString("sensitive")));
        assertThat(jsonExcludeSensitiveAndBuiltInCompact, not(containsString(ServiceDocument.FIELD_NAME_VERSION)));
        assertFalse("compact JSON should not have whitespaces",
                containsWhitespacePattern.matcher(jsonExcludeSensitiveAndBuiltInCompact).find());
    }
}
