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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import com.google.gson.reflect.TypeToken;

import org.junit.Assert;
import org.junit.Test;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.SystemHostInfo.OsFamily;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;

public class TestUtils {

    public int iterationCount = 1000;

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
    }

    @Test
    public void toHexString() {
        byte[] bytes = new byte[4];
        bytes[0] = 0x12;
        bytes[1] = 0x34;
        bytes[2] = (byte) 0xAB;
        bytes[3] = (byte) 0xCD;

        String out = Utils.toHexString(bytes);
        assertEquals("1234abcd", out);
    }

    @Test
    public void toHexStringZeroes() {
        byte[] bytes = new byte[4];
        bytes[0] = 0x00;
        bytes[1] = 0x00;
        bytes[2] = 0x00;
        bytes[3] = 0x00;

        String out = Utils.toHexString(bytes);
        assertEquals("00000000", out);
    }

    @Test
    public void toBytes() {
        final int expectedByteCount = 115;
        int count = 100000;
        ServiceDocument s = buildCloneOrSerializationObject();

        int byteCount = 0;
        long start = Utils.getNowMicrosUtc();
        byte[] content = new byte[1024];
        for (int i = 0; i < count; i++) {
            byteCount = Utils.toBytes(s, content, 0);
            assertTrue(content != null);
            assertTrue(content.length >= expectedByteCount);
        }

        long end = Utils.getNowMicrosUtc();
        double thpt = end - start;
        thpt /= 1000000;
        thpt = count / thpt;

        Logger.getAnonymousLogger().info(
                String.format(
                        "Binary serializations per second: %f, iterations: %d, byte count: %d",
                        thpt, count, byteCount));
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
        long l = Utils.getNowMicrosUtc() + 1000;
        assertTrue(Utils.isWithinTimeComparisonEpsilon(l));
        // check a value days from now
        l = Utils.getNowMicrosUtc() + TimeUnit.DAYS.toMicros(2);
        assertFalse(Utils.isWithinTimeComparisonEpsilon(l));
    }

    @Test
    public void cloneDocumentAndObject() {
        int count = 100000;
        Object s = buildCloneOrSerializationObject();

        long start = Utils.getNowMicrosUtc();
        for (int i = 0; i < count; i++) {
            s = Utils.cloneObject(s);
            Object foo = s;
            foo = Utils.cloneObject(foo);
        }

        long end = Utils.getNowMicrosUtc();
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

    @Test
    public void fromDocumentBytes() {
        int count = 100000;
        ExampleServiceState s = buildCloneOrSerializationObject();

        byte[] content = new byte[1024];
        int byteCount = Utils.toBytes(s, content, 0);

        long start = Utils.getNowMicrosUtc();
        for (int i = 0; i < count; i++) {
            ExampleServiceState s1 = (ExampleServiceState) Utils.fromDocumentBytes(content, 0,
                    content.length);
            assertEquals(s.counter, s1.counter);
            assertEquals(s.name, s1.name);
            assertEquals(s.keyValues.size(), s1.keyValues.size());
            assertEquals(s.keyValues.get("1"), s1.keyValues.get("1"));
        }

        long end = Utils.getNowMicrosUtc();

        double thpt = end - start;
        thpt /= 1000000;
        thpt = count / thpt;

        Logger.getAnonymousLogger().info(
                String.format(
                        "Binary deserializations per second: %f, iterations: %d, byte count: %d",
                        thpt, count, byteCount));

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
        assertTrue(false == ServiceDocument.equals(desc, original, changed));

        changed = Utils.clone(original);
        changed.stringValue = UUID.randomUUID().toString();
        assertTrue(false == ServiceDocument.equals(desc, original, changed));

        changed = Utils.clone(original);
        changed.mapOfStrings.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        assertTrue(false == ServiceDocument.equals(desc, original, changed));

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

        long s = Utils.getNowMicrosUtc();
        long length = 0;

        for (int i = 0; i < count; i++) {
            if (useBinary) {
                byte[] bytes = new byte[4096];
                int pos = Utils.toBytes(original, bytes, 0);
                assertTrue(bytes != null && pos > 10);
                length = pos;
                Utils.fromDocumentBytes(bytes, 0, pos);
            } else {
                String serializedDocument = Utils.toJson(original);
                assertTrue(serializedDocument != null);
                length = serializedDocument.getBytes().length;
                Utils.fromJson(serializedDocument, original.getClass());
            }
        }
        long e = Utils.getNowMicrosUtc();
        double throughput = (e - s) / (double) TimeUnit.SECONDS.toMicros(1);
        throughput = count / throughput;
        Logger.getAnonymousLogger().info(
                String.format(
                        "Binary: %s, PODO: %s, Ser+des+signature per second: %f, byte count: %d",
                        useBinary, original.getClass().getSimpleName(),
                        throughput, length));
    }

    public QueryValidationServiceState serializedAndCompareDocuments(
            boolean useBinary, QueryValidationServiceState original)
            throws Throwable {
        QueryValidationServiceState originalDeserializedWithSig = null;
        if (useBinary) {
            byte[] serializedDocument = new byte[4096];
            Utils.toBytes(original, serializedDocument, 0);
            originalDeserializedWithSig = (QueryValidationServiceState) Utils.fromBytes(
                    serializedDocument,
                    0, serializedDocument.length);
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

        assertEquals(expected, Utils.getOsName(systemHostInfo));
    }

    @Test
    public void testDetermineOsFamilyForWindows() throws Exception {
        final String osName = "Windows NT";

        assertEquals(OsFamily.WINDOWS, Utils.determineOsFamily(osName));
    }

    @Test
    public void testDetermineOsFamilyForLinux() throws Exception {
        final String osName = "Linux";

        assertEquals(OsFamily.LINUX, Utils.determineOsFamily(osName));
    }

    @Test
    public void testDetermineOsFamilyForMac() throws Exception {
        final String osName = "Mac OS X";

        assertEquals(OsFamily.MACOS, Utils.determineOsFamily(osName));
    }

    @Test
    public void testDetermineOsFamilyForOther() throws Exception {
        final String osName = "TI 99/4A";

        assertEquals(OsFamily.OTHER, Utils.determineOsFamily(osName));
    }

    @Test
    public void testDetermineOsFamilyForNull() throws Exception {
        assertEquals(OsFamily.OTHER, Utils.determineOsFamily(null));
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
    public void testHash() {
        String string1 = "foofoo";
        String string2 = "barbar";
        Assert.assertEquals(Utils.computeHash(string1), Utils.computeHash(string1));
        Assert.assertNotEquals(Utils.computeHash(string1), Utils.computeHash(string2));
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
    public void testDecodeGzipedBody() throws Exception {
        String body = "This is the original body content, bot gzipped";
        byte[] gzippedBody = compress(body);

        Operation op = Operation
                .createGet(null)
                .setContentLength(gzippedBody.length)
                .addResponseHeader(Operation.CONTENT_ENCODING_HEADER,
                        Operation.CONTENT_ENCODING_GZIP)
                .addResponseHeader(Operation.CONTENT_TYPE_HEADER, Operation.MEDIA_TYPE_TEXT_PLAIN);

        Utils.decodeBody(op, ByteBuffer.wrap(gzippedBody));

        assertEquals(body, op.getBody(String.class));

        // Content encoding header is removed as the body is already decoded
        assertNull(op.getResponseHeader(Operation.CONTENT_ENCODING_HEADER));
    }

    @Test
    public void testFailsDecodeGzipedBodyWithoutContentEncoding() throws Exception {
        byte[] gzippedBody = compress("test");

        Operation op = Operation
                .createGet(null)
                .setContentLength(gzippedBody.length)
                .addResponseHeader(Operation.CONTENT_TYPE_HEADER, Operation.MEDIA_TYPE_TEXT_PLAIN);

        Utils.decodeBody(op, ByteBuffer.wrap(gzippedBody));

        assertEquals(Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD, op.getStatusCode());
    }

    private static byte[] compress(String str) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(str.getBytes(Utils.CHARSET));
        gzip.close();
        return out.toByteArray();
    }
}
