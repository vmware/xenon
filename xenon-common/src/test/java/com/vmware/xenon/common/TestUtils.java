/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.MalformedInputException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.VersionFieldSerializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.SystemHostInfo.OsFamily;
import com.vmware.xenon.common.serialization.KryoSerializers;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryValidationTestService.NestedType;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;

public class TestUtils {

    public int iterationCount = 1000;
    /**
     * enables hash collisions in the computeHash methods. Should be set to false when running
     * large iteration count benchmarks
     */
    public boolean checkHashCollisions = true;

    @Rule
    public TestResults testResults = new TestResults();

    @Test
    public void registerKind() {
        String kindBefore = Utils.buildKind(ExampleServiceState.class);
        String newKind = "e";
        Utils.registerKind(ExampleServiceState.class, newKind);
        String kindAfter = Utils.buildKind(ExampleServiceState.class);
        assertEquals(newKind, kindAfter);
        Utils.registerKind(ExampleServiceState.class, kindBefore);
        kindAfter = Utils.buildKind(ExampleServiceState.class);
        assertEquals(kindBefore, kindAfter);
        Class<?> stateClass = Utils.getTypeFromKind(kindAfter);
        assertEquals(stateClass.getCanonicalName(), ExampleServiceState.class.getCanonicalName());
    }

    @Test
    public void buildUUID() {
        CommandLineArgumentParser.parseFromProperties(this);
        Utils.setTimeDriftThreshold(TimeUnit.HOURS.toMicros(1));
        try {
            Logger log = Logger.getAnonymousLogger();
            String baseId = Utils.computeHash("some id");
            // verify uniqueness of each value returned, across N threads
            Set<String> set = new ConcurrentSkipListSet<>();
            int threadCount = Utils.DEFAULT_THREAD_COUNT;
            int iterations = this.iterationCount;
            log.info("Starting uniqueness check, thread count: " + threadCount);
            TestContext ctx = new TestContext(threadCount, Duration.ofSeconds(30));
            for (int t = 0; t < threadCount; t++) {
                ForkJoinPool.commonPool().execute(() -> {
                    for (int i = 0; i < iterations / threadCount; i++) {
                        String value = Utils.buildUUID(baseId);
                        if (!set.add(value)) {
                            ctx.fail(new IllegalStateException("Duplicate: " + value));
                        }
                    }
                    ctx.complete();
                });
            }
            ctx.await();
            set.clear();
            System.gc();

            // keep jvm from optimizing away calls
            int sum = 0;
            long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                sum += Utils.buildUUID(baseId).length();
            }
            long end = System.nanoTime();

            log.info("iterations: " + iterations);
            log.info("Total chars: " + sum);
            double thpt = this.iterationCount / ((end - start) / 1000000000.0);
            log.info("Throughput (calls / sec): " + thpt);
            this.testResults.getReport().lastValue(TestResults.KEY_THROUGHPUT, thpt);
        } finally {
            Utils.setTimeDriftThreshold(Utils.DEFAULT_TIME_DRIFT_THRESHOLD_MICROS);
        }
    }

    @Test
    public void computeStringHash() {
        CommandLineArgumentParser.parseFromProperties(this);
        Set<String> keys = new HashSet<>(this.iterationCount);
        Set<Long> hashedKeys = new HashSet<>(this.iterationCount);
        long collisionCount = 0;

        for (int i = 0; i < this.iterationCount; i++) {
            String k = "-string-" + i;
            long hash = FNVHash.compute(k);
            long hash2 = FNVHash.compute(k);
            assertEquals(hash, hash2);
            assertTrue(keys.add(k));
        }
        Logger.getAnonymousLogger().info("Generated keys: " + keys.size());
        long s = System.nanoTime() / 1000;
        for (String key : keys) {
            long hash = FNVHash.compute(key);
            assertTrue(hash != 0);
            if (this.checkHashCollisions && !hashedKeys.add(hash)) {
                collisionCount++;
            }
        }
        long e = System.nanoTime() / 1000;
        double thpt = this.iterationCount / ((e - s) / 1000000.0);

        this.testResults.getReport().lastValue(TestResults.KEY_THROUGHPUT, thpt);
        Logger.getAnonymousLogger().info("Throughput: " + thpt);
        Logger.getAnonymousLogger().info("Collisions: " + collisionCount);
    }


    private static String computeHash(byte[] content, int offset, int length) {
        return Long.toHexString(FNVHash.compute(content, offset, length));
    }

    @Test
    public void computeByteHash() throws UnsupportedEncodingException {
        CommandLineArgumentParser.parseFromProperties(this);
        Set<String> keys = new HashSet<>(this.iterationCount);
        Set<Long> hashedKeys = new HashSet<>(this.iterationCount);
        long collisionCount = 0;

        for (int i = 0; i < this.iterationCount; i++) {
            String k = "-string-" + i;
            byte[] bytes = k.getBytes(Utils.CHARSET);
            String stringHash = computeHash(bytes, 0, bytes.length);
            String stringHash2 = computeHash(bytes, 0, bytes.length);
            assertEquals(stringHash, stringHash2);
            assertTrue(keys.add(k));
        }
        Logger.getAnonymousLogger().info("Generated keys: " + keys.size());
        long s = System.nanoTime() / 1000;
        for (String key : keys) {
            byte[] bytes = key.getBytes(Utils.CHARSET);
            long hash = FNVHash.compute(bytes, 0, bytes.length);
            assertTrue(hash != 0);
            if (!hashedKeys.add(hash)) {
                collisionCount++;
            }
        }
        long e = System.nanoTime() / 1000;
        double thpt = this.iterationCount / ((e - s) / 1000000.0);
        Logger.getAnonymousLogger().info("Throughput: " + thpt);
        Logger.getAnonymousLogger().info("Collisions: " + collisionCount);
    }

    @Test
    public void buildKind() {
        CommandLineArgumentParser.parseFromProperties(this);
        String kind = Utils.buildKind(ExampleServiceState.class);
        long s = System.nanoTime() / 1000;
        for (int i = 0; i < this.iterationCount; i++) {
            String k = Utils.buildKind(ExampleServiceState.class);
            assertTrue(kind.hashCode() == k.hashCode());
        }
        long e = System.nanoTime() / 1000;
        double thpt = this.iterationCount / ((e - s) / 1000000.0);
        Logger.getAnonymousLogger().info("Throughput: " + thpt);
    }

    @Test
    public void getSystemNowMicrosUtc() {
        CommandLineArgumentParser.parseFromProperties(this);
        long s = System.nanoTime() / 1000;
        for (int i = 0; i < this.iterationCount; i++) {
            Utils.getSystemNowMicrosUtc();
        }
        long e = System.nanoTime() / 1000;
        double thpt = this.iterationCount / ((e - s) / 1000000.0);
        Logger.getAnonymousLogger().info("Throughput: " + thpt);
    }

    @Test
    public void toBytes() {
        final int expectedByteCount = 115;
        int count = 100000;
        ServiceDocument s = buildCloneOrSerializationObject();

        int byteCount = 0;
        long start = System.nanoTime() / 1000;
        for (int i = 0; i < count; i++) {
            byte[] content = Utils.getBuffer(1024);
            byteCount = Utils.toBytes(s, content, 0);
            assertTrue(content != null);
            assertTrue(content.length >= expectedByteCount);
        }

        long end = System.nanoTime() / 1000;
        double thpt = end - start;
        thpt /= 1000000;
        thpt = count / thpt;

        Logger.getAnonymousLogger().info(
                String.format(
                        "Binary serializations per second: %f, iterations: %d, byte count: %d",
                        thpt, count, byteCount));
        this.testResults.getReport().lastValue(TestResults.KEY_THROUGHPUT, thpt);
    }

    @Test
    public void setAndGetTimeComparisonEpsilon() {
        long l = Utils.getTimeComparisonEpsilonMicros();
        assertTrue(l > TimeUnit.SECONDS.toMicros(1));
        l = 41;
        // implicitly set epsilon through JVM property
        System.setProperty(Utils.PROPERTY_NAME_PREFIX +
                Utils.PROPERTY_NAME_TIME_COMPARISON, "" + l);
        Utils.resetTimeComparisonEpsilonMicros();
        long k = Utils.getTimeComparisonEpsilonMicros();
        assertEquals(k, l);
        // explicitly set epsilon
        l = 45;
        Utils.setTimeComparisonEpsilonMicros(l);
        k = Utils.getTimeComparisonEpsilonMicros();
        assertEquals(k, l);
    }

    @Test
    public void isWithinTimeComparisonEpsilon() {
        Utils.setTimeComparisonEpsilonMicros(TimeUnit.SECONDS.toMicros(10));
        // check a value within about a millisecond from now
        long l = Utils.getSystemNowMicrosUtc() + 1000;
        assertTrue(Utils.isWithinTimeComparisonEpsilon(l));
        // check a value days from now
        l = Utils.getSystemNowMicrosUtc() + TimeUnit.DAYS.toMicros(2);
        assertFalse(Utils.isWithinTimeComparisonEpsilon(l));
    }

    public static class CustomKryoForObjectThreadLocal extends ThreadLocal<Kryo> {
        @Override
        protected Kryo initialValue() {
            return createKryo(true);
        }
    }

    public static class CustomKryoForDocumentThreadLocal extends ThreadLocal<Kryo> {
        @Override
        protected Kryo initialValue() {
            return createKryo(false);
        }
    }

    private static final int EXAMPLE_SERVICE_CLASS_ID = 1234;

    private static Kryo createKryo(boolean isObjectSerializer) {
        Kryo k = new Kryo();
        // handle classes with missing default constructors
        k.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        // supports addition of fields if the @since annotation is used
        k.setDefaultSerializer(VersionFieldSerializer.class);

        if (!isObjectSerializer) {
            // For performance reasons, and to avoid memory use, assume documents do not
            // require object graph serialization with duplicate or recursive references
            k.setReferences(false);
        } else {
            // To avoid monotonic increase of memory use, due to reference tracking, we must
            // reset after each use.
            k.setAutoReset(true);
        }

        k.register(ExampleServiceState.class, EXAMPLE_SERVICE_CLASS_ID);
        HashMap<String, String> map = new HashMap<>();
        k.register(map.getClass());
        return k;
    }

    /**
     * This test detects accidental changes to how the signature is calculated. Change it
     * if signature calculation has changed, otherwise it is a regression.
     */
    @Test
    public void testSignature() {
        CommandLineArgumentParser.parseFromProperties(this);
        ServiceDocumentDescription desc = Builder.create()
                .buildDescription(QueryValidationServiceState.class);

        QueryValidationServiceState document = new QueryValidationServiceState();
        document.nestedComplexValue = new NestedType();
        document.nestedComplexValue.id = "document.nestedComplexValue.id";
        document.nestedComplexValue.longValue = Long.MIN_VALUE;
        document.documentKind = Utils.buildKind(document.getClass());
        document.documentSelfLink = "documentSelfLink";
        document.documentVersion = 0;
        document.documentExpirationTimeMicros = 1111111111;
        document.documentSourceLink = "documentSourceLink";
        document.documentOwner = "owner";
        document.documentUpdateTimeMicros = 222222222;
        document.documentAuthPrincipalLink = "documentAuthPrincipalLink";
        document.documentUpdateAction = "PUT";

        document.referenceValue = URI.create("http://www.example.com");
        document.mapOfStrings = new LinkedHashMap<>();
        document.mapOfStrings.put("key1", "value1");
        document.binaryContent = document.documentKind.getBytes(Utils.CHARSET_OBJECT);
        document.booleanValue = false;
        document.doublePrimitive = 3;
        document.doubleValue = Double.valueOf(3);
        document.id = document.documentSelfLink;
        document.serviceLink = document.documentSelfLink;
        document.dateValue = new Date(1422344512424L);
        document.listOfStrings = Arrays.asList("1", "2", "3");

        String wellKnownSignature = "6f06213cfdb5a51a";

        Logger.getAnonymousLogger().info(
                String.format(
                        "Expected signature (%s) of document:\n%s",
                        wellKnownSignature, Utils.toJsonHtml(document)));
        String sig = Utils.computeSignature(document, desc);
        assertEquals(wellKnownSignature, sig);
    }

    @Test
    public void registerCustomKryoSerializer() {
        try {

            ExampleServiceState st = new ExampleServiceState();
            st.id = UUID.randomUUID().toString();
            st.counter = Utils.getNowMicrosUtc();
            st.documentSelfLink = st.id;
            st.keyValues = new HashMap<>();
            st.keyValues.put(st.id, st.id);
            // we need to prove that the default serializer, for both object and document is
            // used and produces a different result, compared to the custom serializer. we first
            // serialize with defaults, then with custom, and compare sizes

            int byteCountToObjectDefault = Utils.toBytes(st, Utils.getBuffer(1024), 0);
            Output outDocumentImplicitDefault = KryoSerializers.serializeAsDocument(st,
                    1024);
            int pDefaultImplicit = outDocumentImplicitDefault.position();
            Output outDocumentDefault = KryoSerializers.serializeDocument(st, 1024);
            int pDefault = outDocumentDefault.position();

            Utils.registerCustomKryoSerializer(new CustomKryoForObjectThreadLocal(), false);
            Utils.registerCustomKryoSerializer(new CustomKryoForDocumentThreadLocal(),
                    true);

            byte[] objectData = new byte[1024];
            int byteCountToObjectCustom = Utils.toBytes(st, objectData, 0);
            Output outDocumentImplicitCustom = KryoSerializers.serializeAsDocument(st,
                    1024);
            int p = outDocumentImplicitCustom.position();
            Output outDocumentCustom = KryoSerializers.serializeDocument(st, 1024);

            assertTrue(byteCountToObjectCustom != byteCountToObjectDefault);
            assertTrue(p != pDefaultImplicit);
            assertTrue(outDocumentCustom.position() != pDefault);

            ExampleServiceState stDeserializedFromObject = (ExampleServiceState) Utils.fromBytes(
                    objectData);
            ExampleServiceState stDeserializedImplicit =
                    (ExampleServiceState) KryoSerializers.deserializeDocument(
                            outDocumentImplicitDefault.getBuffer(), 0,
                            outDocumentImplicitDefault.position());
            ExampleServiceState stDeserialized = (ExampleServiceState) KryoSerializers
                    .deserializeDocument(
                    outDocumentDefault.getBuffer(), 0, outDocumentDefault.position());
            assertEquals(st.id, stDeserializedFromObject.id);
            assertEquals(st.id, stDeserializedImplicit.id);
            assertEquals(st.id, stDeserialized.id);
            assertEquals(st.id, stDeserializedFromObject.keyValues.get(st.id));
            assertEquals(st.id, stDeserializedImplicit.keyValues.get(st.id));
            assertEquals(st.id, stDeserialized.keyValues.get(st.id));
        } finally {
            Utils.registerCustomKryoSerializer(null, false);
            Utils.registerCustomKryoSerializer(null, true);
        }
    }

    @Test
    public void cloneDocumentAndObject() {
        int count = 100000;
        Object s = buildCloneOrSerializationObject();

        long start = System.nanoTime() / 1000;
        for (int i = 0; i < count; i++) {
            s = Utils.cloneObject(s);
            Object foo = s;
            foo = Utils.cloneObject(foo);
        }

        long end = System.nanoTime() / 1000;
        double thpt = end - start;
        thpt /= 1000000;
        thpt = count / thpt;

        Logger.getAnonymousLogger().info(
                String.format(
                        "Clones per second: %f, iterations: %d",
                        thpt, count));
    }

    @Test
    public void fromBytes() {
        int count = 100000;
        Map<String, Long> s = new HashMap<>();
        s.put(UUID.randomUUID().toString(), 1L);
        s.put(UUID.randomUUID().toString(), 2L);

        byte[] content = new byte[1024];
        Utils.toBytes(s, content, 0);
        for (int i = 0; i < count; i++) {
            @SuppressWarnings("unchecked")
            Map<String, Long> s1 = (Map<String, Long>) Utils.fromBytes(content, 0,
                    content.length);
            assertEquals(s.size(), s1.size());
            for (Entry<String, Long> e : s1.entrySet()) {
                assertEquals(s.get(e.getKey()), e.getValue());
            }
        }
    }

    private ExampleServiceState buildCloneOrSerializationObject() {
        ExampleServiceState s = new ExampleServiceState();
        s.counter = 1L;
        s.keyValues = new HashMap<>();
        s.keyValues.put("1", "one");
        s.name = "name";
        return s;
    }

    @Test
    public void toJsonWithSignature() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        doSerializationWithSignature(false);
    }

    @Test
    public void toBytesWithSignature() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        doSerializationWithSignature(true);
    }

    private void doSerializationWithSignature(boolean useBinary) throws Throwable {
        ServiceDocumentDescription desc = buildStateDescription(QueryValidationServiceState.class,
                QueryValidationServiceState.FIELD_NAME_IGNORED_STRING_VALUE);

        QueryValidationServiceState document = VerificationHost.buildQueryValidationState();
        document.documentKind = Utils.buildKind(document.getClass());
        document.documentSelfLink = UUID.randomUUID().toString();
        document.documentVersion = 0;
        document.documentExpirationTimeMicros = Utils.getNowMicrosUtc();
        document.documentSourceLink = UUID.randomUUID().toString();
        document.documentOwner = UUID.randomUUID().toString();
        document.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        document.documentAuthPrincipalLink = UUID.randomUUID().toString();
        document.documentUpdateAction = UUID.randomUUID().toString();
        document.mapOfStrings = new LinkedHashMap<String, String>();
        document.mapOfStrings.put("key1", "value1");

        QueryValidationServiceState original = Utils.clone(document);

        // now we run some experiments to verify the signature calculation does not include core
        // document fields
        document.documentSelfLink = UUID.randomUUID().toString();
        assertTrue(ServiceDocument.equals(desc, original, document));

        document.documentKind = UUID.randomUUID().toString();
        assertTrue(ServiceDocument.equals(desc, original, document));

        document.documentSourceLink = UUID.randomUUID().toString();
        assertTrue(ServiceDocument.equals(desc, original, document));

        document.documentOwner = UUID.randomUUID().toString();
        assertTrue(ServiceDocument.equals(desc, original, document));

        document.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        assertTrue(ServiceDocument.equals(desc, original, document));

        document.documentVersion = Utils.getNowMicrosUtc();
        assertTrue(ServiceDocument.equals(desc, original, document));

        document.documentUpdateAction = UUID.randomUUID().toString();
        assertTrue(ServiceDocument.equals(desc, original, document));

        document.documentAuthPrincipalLink = UUID.randomUUID().toString();
        assertTrue(ServiceDocument.equals(desc, original, document));

        // we have marked one of the derived fields as EXCLUDE from signature, verify that changing
        // it does not change the signature
        document.ignoredStringValue = Utils.getNowMicrosUtc() + "";
        assertTrue(ServiceDocument.equals(desc, original, document));

        // now change derived fields and expect the signature to change
        QueryValidationServiceState changed = Utils.clone(original);
        changed.documentExpirationTimeMicros = Utils.getNowMicrosUtc();
        assertFalse(ServiceDocument.equals(desc, original, changed));

        changed = Utils.clone(original);
        changed.textValue = UUID.randomUUID().toString();
        assertFalse(ServiceDocument.equals(desc, original, changed));

        changed = Utils.clone(original);
        changed.mapOfStrings.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        assertFalse(ServiceDocument.equals(desc, original, changed));

        // finally do a simple throughput test
        logThroughput(this.iterationCount, useBinary, desc, original);
        logThroughput(this.iterationCount, useBinary, desc, original);
        logThroughput(this.iterationCount, useBinary, desc, original);

        desc = buildStateDescription(ExampleServiceState.class, null);
        ExampleServiceState s = new ExampleServiceState();
        s.name = UUID.randomUUID().toString();
        s.counter = 5L;
        s.keyValues = new HashMap<>();
        s.keyValues.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        logThroughput(this.iterationCount, useBinary, desc, s);
        logThroughput(this.iterationCount, useBinary, desc, s);
        logThroughput(this.iterationCount, useBinary, desc, s);
    }

    public void logThroughput(int count, boolean useBinary, ServiceDocumentDescription desc,
            ServiceDocument original)
            throws Throwable {

        long s = System.nanoTime() / 1000;
        long length = 0;

        for (int i = 0; i < count; i++) {
            if (useBinary) {
                Output o = KryoSerializers.serializeDocument(original, 4096);
                assertTrue(o != null && o.position() > 10);
                length = o.position();
                KryoSerializers.deserializeDocument(o.getBuffer(), 0, o.position());
            } else {
                String serializedDocument = Utils.toJson(original);
                assertTrue(serializedDocument != null);
                length = serializedDocument.getBytes().length;
                Utils.fromJson(serializedDocument, original.getClass());
            }
        }
        long e = System.nanoTime() / 1000;
        double throughput = (e - s) / (double) TimeUnit.SECONDS.toMicros(1);
        throughput = count / throughput;
        Logger.getAnonymousLogger().info(
                String.format(
                        "Binary: %s, PODO: %s, Ser+des+signature per second: %f, byte count: %d",
                        useBinary, original.getClass().getSimpleName(),
                        throughput, length));
    }

    @Test
    public void signatureThroughput() {
        CommandLineArgumentParser.parseFromProperties(this);
        ServiceDocumentDescription desc = Builder.create()
                .buildDescription(QueryValidationServiceState.class);

        QueryValidationServiceState document = VerificationHost.buildQueryValidationState();
        document.documentKind = Utils.buildKind(document.getClass());
        document.documentSelfLink = UUID.randomUUID().toString();
        document.documentVersion = 0;
        document.documentExpirationTimeMicros = Utils.getNowMicrosUtc();
        document.documentSourceLink = UUID.randomUUID().toString();
        document.documentOwner = UUID.randomUUID().toString();
        document.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        document.documentAuthPrincipalLink = UUID.randomUUID().toString();
        document.documentUpdateAction = UUID.randomUUID().toString();

        document.mapOfStrings = new LinkedHashMap<>();
        document.mapOfStrings.put("key1", "value1");
        document.mapOfStrings.put("key2", "value2");
        document.mapOfStrings.put("key3", "value3");
        document.binaryContent = document.documentKind.getBytes();
        document.booleanValue = false;
        document.doublePrimitive = 3;
        document.doubleValue = Double.valueOf(3);
        document.id = document.documentSelfLink;
        document.serviceLink = document.documentSelfLink;
        document.dateValue = new Date();
        document.listOfStrings = Arrays.asList("1", "2", "3", "4", "5");

        long start = System.nanoTime();
        for (int i = 0; i < this.iterationCount; i++) {
            Utils.computeSignature(document, desc);
        }

        long duration = System.nanoTime() - start;

        double thpt = this.iterationCount * 1000.0 * 1000.0 * 1000.0 / duration;
        Logger.getAnonymousLogger().info(
                String.format(
                        "Signature calculation throughput: %.2f/sec", thpt));
        this.testResults.getReport().lastValue(TestResults.KEY_THROUGHPUT, thpt);
    }

    public QueryValidationServiceState serializedAndCompareDocuments(
            boolean useBinary, QueryValidationServiceState original)
            throws Throwable {
        QueryValidationServiceState originalDeserializedWithSig = null;
        if (useBinary) {
            Output o = KryoSerializers.serializeDocument(original, 4096);
            originalDeserializedWithSig = (QueryValidationServiceState) KryoSerializers
                    .deserializeDocument(
                            o.getBuffer(),
                            0, o.position());
        } else {
            String serializedDocument = Utils.toJson(original);
            originalDeserializedWithSig = Utils.fromJson(
                    serializedDocument,
                    QueryValidationServiceState.class);
        }
        compareDocumentFields(original, originalDeserializedWithSig);
        return originalDeserializedWithSig;
    }

    public static ServiceDocumentDescription buildStateDescription(
            Class<? extends ServiceDocument> type,
            String excludeFieldName) {
        EnumSet<ServiceOption> options = EnumSet.of(ServiceOption.PERSISTENCE);
        ServiceDocumentDescription desc = Builder.create()
                .buildDescription(type, options);

        // exclude in indexing / signature
        if (excludeFieldName != null) {
            desc.propertyDescriptions.get(excludeFieldName).indexingOptions = EnumSet
                    .of(PropertyIndexingOption.EXCLUDE_FROM_SIGNATURE);
        }

        return desc;
    }

    public void compareDocumentFields(QueryValidationServiceState original,
            QueryValidationServiceState deserializedWithSig) throws IllegalAccessException {
        for (Field f : deserializedWithSig.getClass().getFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            Object afterValue = f.get(deserializedWithSig);
            Object originalValue = f.get(original);

            if (f.getName().equals(ServiceDocument.FIELD_NAME_DESCRIPTION)) {
                // description is expected to be null for the serialized document since it
                // targeting the index
                assertTrue(afterValue == null);
                continue;
            }

            if (originalValue == null && afterValue == null) {
                continue;
            }

            assertEquals(Utils.toJson(originalValue), Utils.toJson(afterValue));
        }
    }

    private static final int NUM_THREADS = 2;
    private static final int NUM_ITERATIONS = 100;

    @Test
    public void benchmarkGetNowMicrosUtc() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Thread[] threads = new Thread[NUM_THREADS];
        final Exception[] e = { null };

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    // Wait for test thread to give the start signal
                    try {
                        latch.await();
                    } catch (InterruptedException ie) {
                        e[0] = ie;
                        return;
                    }

                    long max = 0;
                    for (int i = 0; i < NUM_ITERATIONS; i++) {
                        long now = Utils.getNowMicrosUtc();
                        if (now < max) {
                            e[0] = new Exception("Time moved backwards:" + now + " < " + max);
                            return;
                        }
                        max = now;
                    }
                }
            };
            threads[i].start();
        }

        long start;
        long stop;

        start = System.nanoTime();
        latch.countDown();
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        stop = System.nanoTime();

        assertNull(e[0]);

        System.out.println(String.format("Elapsed: %dms",
                TimeUnit.NANOSECONDS.toMillis(stop - start)));
    }

    private static class TestKeyObjectValueHolder {
        private final Map<String, Object> keyValues = new HashMap<>();
    }

    private void checkOptions(EnumSet<Service.ServiceOption> options) {
        checkOptions(options, false);
    }

    private void checkOptions(EnumSet<Service.ServiceOption> options, boolean isFailureExpected) {
        String error;
        for (Service.ServiceOption o : options) {
            error = Utils.validateServiceOption(options, o);
            if (error != null && !isFailureExpected) {
                throw new IllegalArgumentException(error);
            }
        }
    }

    @Test
    public void toJsonHtml() {
        ServiceDocument doc = new ServiceDocument();
        String json = Utils.toJsonHtml(doc);
        assertTrue(json.contains("  "));
    }

    @Test
    public void toJsonHtmlNull() {
        String json = Utils.toJsonHtml(null);
        assertEquals(json, "null");
    }

    @Test
    public void validateServiceOption() {
        // positive tests
        EnumSet<ServiceOption> options = EnumSet.of(ServiceOption.REPLICATION,
                ServiceOption.OWNER_SELECTION);
        checkOptions(options);

        options = EnumSet.of(ServiceOption.REPLICATION, ServiceOption.OWNER_SELECTION,
                ServiceOption.INSTRUMENTATION, ServiceOption.PERIODIC_MAINTENANCE,
                ServiceOption.HTML_USER_INTERFACE);
        checkOptions(options);

        options = EnumSet.of(ServiceOption.REPLICATION, ServiceOption.STRICT_UPDATE_CHECKING);
        checkOptions(options);

        options = EnumSet.of(ServiceOption.REPLICATION, ServiceOption.STRICT_UPDATE_CHECKING,
                ServiceOption.OWNER_SELECTION);
        checkOptions(options);

        // negative tests

        options = EnumSet.of(ServiceOption.CONCURRENT_UPDATE_HANDLING,
                ServiceOption.STRICT_UPDATE_CHECKING);
        checkOptions(options, true);

        options = EnumSet.of(ServiceOption.REPLICATION,
                ServiceOption.URI_NAMESPACE_OWNER);
        checkOptions(options, true);

        options = EnumSet.of(ServiceOption.PERSISTENCE,
                ServiceOption.URI_NAMESPACE_OWNER);
        checkOptions(options, true);

        options = EnumSet.of(ServiceOption.OWNER_SELECTION, ServiceOption.REPLICATION,
                ServiceOption.CONCURRENT_UPDATE_HANDLING);
        checkOptions(options, true);

        options = EnumSet.of(ServiceOption.OWNER_SELECTION);
        checkOptions(options, true);

        options = EnumSet.of(ServiceOption.PERIODIC_MAINTENANCE, ServiceOption.ON_DEMAND_LOAD);
        checkOptions(options, true);

        options = EnumSet.of(ServiceOption.ON_DEMAND_LOAD, ServiceOption.PERSISTENCE);
        checkOptions(options, false);
    }

    @Test
    public void testParseJsonWhenMapWithValueTypeObject() throws Exception {
        TestKeyObjectValueHolder testHolder = Utils.fromJson(
                "{\"keyValues\":{\"prop1\":\"value1\"}}", TestKeyObjectValueHolder.class);
        assertEquals("value1", testHolder.keyValues.get("prop1"));
    }

    @Test
    public void testParseInstant() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.set(2013, 4, 30, 23, 38, 27);
        cal.set(Calendar.MILLISECOND, 85);

        Instant expected = cal.toInstant();

        Instant actual = Utils.fromJson("\"2013-05-30T23:38:27.085Z\"", Instant.class);
        assertEquals(expected, actual);
    }

    @Test
    public void testParseZonedDateTime() throws Exception {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("Australia/Sydney"));
        cal.set(2013, 4, 30, 23, 38, 27);
        cal.set(Calendar.MILLISECOND, 85);

        ZoneId zid = ZoneId.of("Australia/Sydney");

        ZonedDateTime expected = ZonedDateTime.ofInstant(cal.toInstant(), zid);

        ZonedDateTime actual = Utils.fromJson(
                "\"2013-05-30T23:38:27.085+10:00[Australia/Sydney]\"", ZonedDateTime.class);
        assertEquals(expected, actual);
    }

    @Test
    public void testGetOsName() throws Exception {
        final String expected = "Windows Me";

        SystemHostInfo systemHostInfo = new SystemHostInfo();
        systemHostInfo.properties.put(SystemHostInfo.PROPERTY_NAME_OS_NAME, expected);

        assertEquals(expected, systemHostInfo.getOsName());
    }

    @Test
    public void testDetermineOsFamilyForWindows() throws Exception {
        final String osName = "Windows NT";

        assertEquals(OsFamily.WINDOWS, SystemHostInfo.determineOsFamily(osName));
    }

    @Test
    public void testDetermineOsFamilyForLinux() throws Exception {
        final String osName = "Linux";

        assertEquals(OsFamily.LINUX, SystemHostInfo.determineOsFamily(osName));
    }

    @Test
    public void testDetermineOsFamilyForMac() throws Exception {
        final String osName = "Mac OS X";

        assertEquals(OsFamily.MACOS, SystemHostInfo.determineOsFamily(osName));
    }

    @Test
    public void testDetermineOsFamilyForOther() throws Exception {
        final String osName = "TI 99/4A";

        assertEquals(OsFamily.OTHER, SystemHostInfo.determineOsFamily(osName));
    }

    @Test
    public void testDetermineOsFamilyForNull() throws Exception {
        assertEquals(OsFamily.OTHER, SystemHostInfo.determineOsFamily(null));
    }

    @Test
    public void testServiceUiDefaultPath() {
        class MyService extends StatelessService {
        }

        Service s = new MyService();
        s.setHost(new VerificationHost());

        Path path = Paths.get(Utils.UI_DIRECTORY_NAME, Utils.buildServicePath(MyService.class));
        assertEquals(path, Utils.getServiceUiResourcePath(s));
    }

    @Test
    public void testServiceProvidedUiPath() {
        String resourcePath = "ui/exampleService";
        class MyService extends StatelessService {

            @Override
            public ServiceDocument getDocumentTemplate() {
                ServiceDocument serviceDocument = new ServiceDocument();
                serviceDocument.documentDescription = new ServiceDocumentDescription();
                serviceDocument.documentDescription.userInterfaceResourcePath = resourcePath;
                return serviceDocument;
            }
        }

        Path path = Paths.get(resourcePath);

        assertEquals(path, Utils.getServiceUiResourcePath(new MyService()));
    }

    @Test
    public void testServiceProvidedUiPathEmpty() {
        class MyService extends StatelessService {

            @Override
            public ServiceDocument getDocumentTemplate() {
                ServiceDocument serviceDocument = new ServiceDocument();
                serviceDocument.documentDescription = new ServiceDocumentDescription();
                serviceDocument.documentDescription.userInterfaceResourcePath = "";
                return serviceDocument;
            }
        }

        assertNull(Utils.getServiceUiResourcePath(new MyService()));
    }

    @Test
    public void testGetFromPrimitives() {
        Map<String, Object> map = new HashMap<>();
        map.put("bt", true);
        map.put("bf", false);
        map.put("int", 123);
        map.put("double", 3.14);
        map.put("string", "hello");
        ExampleServiceState doc = new ExampleServiceState();
        doc.documentSelfLink = "selfLink";
        doc.tags = new HashSet<>(Arrays.asList("t1", "t2"));
        map.put("doc", doc);

        validateExtract(Utils.toJson(map));
        validateExtract(Utils.fromJson(Utils.toJson(map), JsonElement.class));
    }

    private void validateExtract(Object jsonRepr) {
        assertEquals(true, Utils.getJsonMapValue(jsonRepr, "bt", Boolean.class));
        assertEquals(false, Utils.getJsonMapValue(jsonRepr, "bf", Boolean.class));
        assertEquals(Integer.valueOf(123), Utils.getJsonMapValue(jsonRepr, "int", Integer.class));
        assertEquals(Double.valueOf(3.14), Utils.getJsonMapValue(jsonRepr, "double", Double.class));
        assertEquals("hello", Utils.getJsonMapValue(jsonRepr, "string", String.class));
        assertEquals("selfLink", Utils.getJsonMapValue(jsonRepr, "doc", ExampleServiceState.class).documentSelfLink);
        assertTrue(Utils.getJsonMapValue(jsonRepr, "doc", ExampleServiceState.class).tags.contains("t1"));

        assertNull(Utils.getJsonMapValue(jsonRepr, "badKey", ExampleServiceState.class));

        // coercion to string always possible
        assertEquals("true", Utils.getJsonMapValue(jsonRepr, "bt", String.class));

        try {
            // must fail if object is expected
            Utils.getJsonMapValue(jsonRepr, "bt", ServiceDocument.class);
            fail("Impossible conversion");
        } catch (JsonSyntaxException ignore) {

        }
    }

    @Test
    public void testGetFromJsonMap() {
        String sampleJson = "{"
                + "\"key1\":\"val1\","
                + "\"key2\":[\"val21\", \"val22\"],"
                + "\"key3\":{\"key31\":false,\"key32\":false}"
                + "}";
        String val1 = Utils.getJsonMapValue(sampleJson, "key1", String.class);
        assertTrue(val1.equals("val1"));
        List<String> val2 = Utils.getJsonMapValue(sampleJson, "key2",
                new TypeToken<List<String>>() {
                }.getType());
        assertTrue(val2.size() == 2);
        assertTrue(val2.contains("val21"));
        assertTrue(val2.contains("val22"));
        Map<String, Boolean> val3 = Utils.getJsonMapValue(sampleJson, "key3",
                new TypeToken<Map<String, Boolean>>() {
                }.getType());
        assertTrue(val3.size() == 2);
        assertTrue(val3.containsKey("key31"));
        assertTrue(val3.containsKey("key32"));
    }

    @Test
    public void testDecodeUrlEncodedText() throws Throwable {

        String textPlain = "param1=value1&param2=value 2&param3=value три";
        byte[] textEncoded = URLEncoder.encode(textPlain, Utils.CHARSET).getBytes(Utils.CHARSET);

        String textDecoded = Utils.decodeIfText(ByteBuffer.wrap(textEncoded),
                Operation.MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED);

        Assert.assertEquals(textPlain, textDecoded);
    }

    @Test
    public void testValidateStateForUniqueIdentifier() {
        ExampleServiceState state = new ExampleServiceState();
        state.id = null;
        state.required = "testRequiredField";
        ServiceDocumentDescription desc = buildStateDescription(ExampleServiceState.class, null);
        Utils.validateState(desc, state);
        Assert.assertNotNull("Unique Identifier was not provided a default UUID", state.id);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateStateForRequiredField() {
        ExampleServiceState state = new ExampleServiceState();
        state.id = null;
        state.required = null;
        ServiceDocumentDescription desc = buildStateDescription(ExampleServiceState.class, null);
        Utils.validateState(desc, state);
    }

    @Test
    public void testDecodeGzipedResponseBody() throws Exception {
        String body = "This is the original body content, but gzipped";
        byte[] gzippedBody = compress(body);

        Operation op = Operation
                .createGet(null)
                .setContentLength(gzippedBody.length)
                .addResponseHeader(Operation.CONTENT_ENCODING_HEADER,
                        Operation.CONTENT_ENCODING_GZIP)
                .addResponseHeader(Operation.CONTENT_TYPE_HEADER, Operation.MEDIA_TYPE_TEXT_PLAIN);

        Utils.decodeBody(op, ByteBuffer.wrap(gzippedBody), false);

        assertEquals(body, op.getBody(String.class));

        // Content encoding header is removed as the body is already decoded
        assertNull(op.getResponseHeader(Operation.CONTENT_ENCODING_HEADER));
    }

    @Test
    public void testDecodeGzipedRequestBody() throws Exception {
        String body = "This is the original body content, but gzipped";
        byte[] gzippedBody = compress(body);

        Operation op = Operation
                .createGet(null)
                .setContentLength(gzippedBody.length)
                .addRequestHeader(Operation.CONTENT_ENCODING_HEADER,
                        Operation.CONTENT_ENCODING_GZIP)
                .addRequestHeader(Operation.CONTENT_TYPE_HEADER, Operation.MEDIA_TYPE_TEXT_PLAIN);

        Utils.decodeBody(op, ByteBuffer.wrap(gzippedBody), true);

        assertEquals(body, op.getBody(String.class));

        // Content encoding header is removed as the body is already decoded
        assertNull(op.getRequestHeader(Operation.CONTENT_ENCODING_HEADER));
    }

    @Test
    public void testFailsDecodeGzipedBodyWithoutContentEncoding() throws Exception {
        byte[] gzippedBody = compress("test");

        Operation op = Operation
                .createGet(null)
                .setContentLength(gzippedBody.length)
                .addResponseHeader(Operation.CONTENT_TYPE_HEADER, Operation.MEDIA_TYPE_TEXT_PLAIN);
        try {
            Utils.decodeBody(op, ByteBuffer.wrap(gzippedBody), false);
            throw new IllegalStateException("should have failed");
        } catch (MalformedInputException e) {

        }
    }

    private static byte[] compress(String str) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(str.getBytes(Utils.CHARSET));
        gzip.close();
        return out.toByteArray();
    }

    public static class TestInvalidComputerSignatureState extends ServiceDocument {
        public String key1;
        public String key2;
    }

    @Test
    public void testComputeSignature() throws Exception {
        ServiceDocumentDescription sdd = TestUtils.buildStateDescription(
                TestInvalidComputerSignatureState.class, null);

        TestInvalidComputerSignatureState state1 = new TestInvalidComputerSignatureState();
        state1.key1 = "1";
        state1.key2 = null;

        TestInvalidComputerSignatureState state2 = new TestInvalidComputerSignatureState();
        state2.key1 = null;
        state2.key2 = "1";

        TestInvalidComputerSignatureState state3 = new TestInvalidComputerSignatureState();
        state3.key1 = "1";
        state3.key2 = "";

        String sign1 = Utils.computeSignature(state1, sdd);
        String sign2 = Utils.computeSignature(state2, sdd);
        String sign3 = Utils.computeSignature(state3, sdd);

        assertNotEquals(sign1, sign2);
        assertNotEquals(sign2, sign3);
        assertNotEquals(sign1, sign3);
    }

    @Test
    public void testBuildServiceConfig() {
        Service exampleService = new ExampleService();
        exampleService.setHost(VerificationHost.create());

        ServiceConfiguration config = new ServiceConfiguration();
        Utils.buildServiceConfig(config, exampleService);

        assertEquals(exampleService.getOptions(), config.options);
        assertEquals(exampleService.getMaintenanceIntervalMicros(), config.maintenanceIntervalMicros);
        assertEquals(ExampleServiceState.VERSION_RETENTION_LIMIT, config.versionRetentionLimit);
        assertEquals(ExampleServiceState.VERSION_RETENTION_FLOOR, config.versionRetentionFloor);
        assertEquals(exampleService.getPeerNodeSelectorPath(), config.peerNodeSelectorPath);
        assertEquals(exampleService.getDocumentIndexPath(), config.documentIndexPath);
    }

}
