/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.vmware.xenon.common.Utils.MergeResult;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryValidationTestService;

public class TestServiceDocument {

    public static final Integer SOME_INT_VALUE = 100;
    public static final Integer SOME_OTHER_INT_VALUE = 200;
    public static final long SOME_EXPIRATION_VALUE = Utils.getSystemNowMicrosUtc();
    public static final String SOME_STRING_VALUE = "some value";
    public static final String SOME_OTHER_STRING_VALUE = "some other value";
    public static final String SOME_IGNORE_VALUE = "ignore me";
    public static final String SOME_OTHER_IGNORE_VALUE = "ignore me please";
    public static final long SOME_OTHER_EXPIRATION_VALUE =
            Utils.fromNowMicrosUtc(TimeUnit.MINUTES.toMicros(5));

    private static class Range {
        public final int from;
        public final int to;

        public Range(int from, int to) {
            this.from = from;
            this.to = to;
        }
    }

    /**
     * Test merging where patch updates all mergeable fields.
     */
    @Test
    public void testFullMerge() {
        MergeTest source = new MergeTest();
        source.s = SOME_STRING_VALUE;
        source.x = SOME_INT_VALUE;
        source.ignore = SOME_IGNORE_VALUE;
        source.documentExpirationTimeMicros = SOME_EXPIRATION_VALUE;
        source.listOfStrings = new ArrayList<String>();
        source.listOfStrings.add(SOME_STRING_VALUE);
        source.mapOfStrings = new HashMap<String, String>();
        source.mapOfStrings.put(SOME_STRING_VALUE, SOME_STRING_VALUE);
        source.setOfStrings = new HashSet<String>();
        source.setOfStrings.add(SOME_STRING_VALUE);
        source.intArray = new int[2];
        source.intArray[0] = SOME_INT_VALUE;
        source.intArray[1] = SOME_INT_VALUE;
        MergeTest patch = new MergeTest();
        patch.s = SOME_OTHER_STRING_VALUE;
        patch.x = SOME_OTHER_INT_VALUE;
        patch.ignore = SOME_OTHER_IGNORE_VALUE;
        patch.listOfStrings = new ArrayList<String>();
        patch.listOfStrings.add(SOME_STRING_VALUE);
        patch.listOfStrings.add(SOME_OTHER_STRING_VALUE);
        patch.setOfStrings = new HashSet<String>();
        patch.setOfStrings.add(SOME_STRING_VALUE);
        patch.setOfStrings.add(SOME_OTHER_STRING_VALUE);
        patch.mapOfStrings = new HashMap<String, String>();
        patch.mapOfStrings.put(SOME_STRING_VALUE, SOME_STRING_VALUE);
        patch.mapOfStrings.put(SOME_OTHER_STRING_VALUE, SOME_OTHER_STRING_VALUE);
        patch.intArray = new int[2];
        patch.intArray[0] = SOME_INT_VALUE;
        patch.intArray[1] = SOME_INT_VALUE;
        patch.documentExpirationTimeMicros = SOME_OTHER_EXPIRATION_VALUE;

        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertTrue("There should be changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_OTHER_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_OTHER_INT_VALUE);
        Assert.assertEquals("Non-annotated ignore field", source.ignore, SOME_IGNORE_VALUE);
        Assert.assertEquals("Auto-annotated expiration field", source.documentExpirationTimeMicros,
                SOME_OTHER_EXPIRATION_VALUE);
        Assert.assertEquals("Number of list elements", 3, source.listOfStrings.size());
        Assert.assertTrue("Check existence of element", source.listOfStrings.contains(SOME_STRING_VALUE));
        Assert.assertTrue("Check existence of element", source.listOfStrings.contains(SOME_OTHER_STRING_VALUE));
        Assert.assertEquals("Number of set elements", 2, source.setOfStrings.size());
        Assert.assertTrue("Check existence of element", source.setOfStrings.contains(SOME_STRING_VALUE));
        Assert.assertTrue("Check existence of element", source.setOfStrings.contains(SOME_OTHER_STRING_VALUE));
        Assert.assertEquals("Number of map elements", 2, source.mapOfStrings.size());
        Assert.assertTrue("Check existence of element", source.mapOfStrings.containsKey(SOME_STRING_VALUE));
        Assert.assertTrue("Check existence of element", source.mapOfStrings.containsKey(SOME_OTHER_STRING_VALUE));
        Assert.assertEquals("Number of Array elements", 2, source.intArray.length);

        patch.intArray = null;
        patch.listOfStrings = null;
        Assert.assertFalse("Repeated patch should not change source", Utils.mergeWithState(d, source, patch));
    }

    /**
     * Test merging where patch updates all mergeable fields into an object which have all the fields unset.
     */
    @Test
    public void testFullMerge2() {
        MergeTest source = new MergeTest();
        MergeTest patch = new MergeTest();
        patch.s = SOME_OTHER_STRING_VALUE;
        patch.x = SOME_OTHER_INT_VALUE;
        patch.listOfStrings = new ArrayList<String>();
        patch.listOfStrings.add(SOME_OTHER_STRING_VALUE);
        patch.mapOfStrings = new HashMap<String, String>();
        patch.mapOfStrings.put(SOME_OTHER_STRING_VALUE, SOME_OTHER_STRING_VALUE);
        patch.intArray = new int[2];
        patch.intArray[0] = SOME_INT_VALUE;
        patch.intArray[1] = SOME_INT_VALUE;
        patch.ignore = SOME_OTHER_IGNORE_VALUE;
        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertTrue("There should be changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_OTHER_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_OTHER_INT_VALUE);
        Assert.assertNull("Non-annotated ignore field", source.ignore);
        Assert.assertEquals("Number of list elements", 1, source.listOfStrings.size());
        Assert.assertTrue("Check existence of element", source.listOfStrings.contains(SOME_OTHER_STRING_VALUE));
        Assert.assertEquals("Number of map elements", 1, source.mapOfStrings.size());
        Assert.assertTrue("Check existence of element", source.mapOfStrings.containsKey(SOME_OTHER_STRING_VALUE));
        Assert.assertEquals("Number of Array elements", 2, source.intArray.length);

        patch.intArray = null;
        patch.listOfStrings = null;
        Assert.assertFalse("Repeated patch should not change source", Utils.mergeWithState(d, source, patch));
    }

    /**
     * Test merging where patch updates map values. Test if old values are preserved,
     * new values are added, modify values are updated and null values are deleted.
     */
    @Test
    public void testMapMerge() {
        MergeTest source = new MergeTest();
        MergeTest patch = new MergeTest();

        patch.mapOfStrings = new HashMap<String, String>();
        patch.mapOfStrings.put("key-1", "value-1");
        patch.mapOfStrings.put("key-2", "value-2");
        patch.mapOfStrings.put("key-3", "value-3");

        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertTrue("There should be changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Check new map size", source.mapOfStrings.size(), 3);
        Assert.assertEquals("Check new map value 1.", source.mapOfStrings.get("key-1"), "value-1");
        Assert.assertEquals("Check new map value 2.", source.mapOfStrings.get("key-2"), "value-2");
        Assert.assertEquals("Check new map value 3.", source.mapOfStrings.get("key-3"), "value-3");

        patch.mapOfStrings = new HashMap<String, String>();
        patch.mapOfStrings.put("key-2", "value-2-patched");
        patch.mapOfStrings.put("key-3", null);
        patch.mapOfStrings.put("key-4", "value-4-new");

        Assert.assertTrue("There should be changes. One deleted and one added.", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Check map size. One deleted and one added value.", source.mapOfStrings.size(), 3);
        Assert.assertEquals("Check unmodified key/value is preserved.", source.mapOfStrings.get("key-1"), "value-1");
        Assert.assertEquals("Check modified key/value is changed as intended.", source.mapOfStrings.get("key-2"), "value-2-patched");
        Assert.assertEquals("Check new key/value is added.", source.mapOfStrings.get("key-4"), "value-4-new");

        Assert.assertFalse("Repeated patch should not change source", Utils.mergeWithState(d, source, patch));
    }

    @Test
    public void testCollectionsUpdate() throws Throwable {
        MergeTest state = new MergeTest();
        state.listOfStrings = new ArrayList<String>();
        state.listOfStrings.add(SOME_STRING_VALUE);
        state.listOfStrings.add(SOME_OTHER_STRING_VALUE);
        state.setOfStrings = new HashSet<String>();
        Map<String, Collection<Object>> collectionsToRemove = new HashMap<>();
        collectionsToRemove.put("listOfStrings", new ArrayList<>(state.listOfStrings));
        Map<String, Collection<Object>> collectionsToAdd = new HashMap<>();
        collectionsToRemove.put("listOfStrings", new ArrayList<>(state.listOfStrings));
        collectionsToAdd.put("setOfStrings", new ArrayList<>(Arrays.asList(SOME_STRING_VALUE)));
        ServiceStateCollectionUpdateRequest request = ServiceStateCollectionUpdateRequest.create(collectionsToAdd, collectionsToRemove);
        boolean changed = Utils.updateCollections(state, request);
        assertTrue(changed);
        assertEquals(state.listOfStrings.size(), 0);
        assertEquals(state.setOfStrings.size(), 1);

        // repeating the update should not change the state anymore
        changed = Utils.updateCollections(state, request);
        assertFalse(changed);
    }

    @Test
    public void testCollectionsUpdateThroughMergeMethod() throws Throwable {
        MergeTest state = new MergeTest();
        state.setOfStrings = new HashSet<String>();
        Map<String, Collection<Object>> collectionsToAdd = new HashMap<>();
        collectionsToAdd.put("setOfStrings", new ArrayList<>(Arrays.asList(SOME_STRING_VALUE)));
        ServiceStateCollectionUpdateRequest request =
                ServiceStateCollectionUpdateRequest.create(collectionsToAdd, null);

        Operation patchOperation = Operation.createPatch(new URI("http://test")).setBody(request);
        ServiceDocumentDescription desc = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        EnumSet<MergeResult> result = Utils.mergeWithStateAdvanced(desc, state, MergeTest.class,
                patchOperation);
        assertTrue(result.contains(MergeResult.SPECIAL_MERGE));
        assertTrue(result.contains(MergeResult.STATE_CHANGED));
        assertEquals(state.setOfStrings.size(), 1);

        // repeating the update should not change the state anymore
        result = Utils.mergeWithStateAdvanced(desc, state, MergeTest.class, patchOperation);
        assertTrue(result.contains(MergeResult.SPECIAL_MERGE));
        assertFalse(result.contains(MergeResult.STATE_CHANGED));
    }

    @Test
    public void testMapsUpdateThroughMergeMethod() throws Throwable {
        MergeTest state = new MergeTest();

        // add map entries
        Map<Object, Object> newEntries = new HashMap<>();
        newEntries.put("key-1", "value-1");
        newEntries.put("key-2", "value-2");
        Map<String, Map<Object, Object>> entriesToAdd = new HashMap<>();
        entriesToAdd.put("mapOfStrings", newEntries);

        ServiceStateMapUpdateRequest request = ServiceStateMapUpdateRequest.create(entriesToAdd, null);
        Operation patchOperation = Operation.createPatch(new URI("http://test")).setBody(request);
        ServiceDocumentDescription desc = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        EnumSet<MergeResult> result = Utils.mergeWithStateAdvanced(desc, state, MergeTest.class,
                patchOperation);
        assertTrue(result.contains(MergeResult.SPECIAL_MERGE));
        assertTrue(result.contains(MergeResult.STATE_CHANGED));
        assertEquals(state.mapOfStrings.size(), 2);

        // remove map keys
        Map<String, Collection<Object>> keysToRemove = new HashMap<>();
        keysToRemove.put("mapOfStrings", Arrays.asList("key-1"));

        request = ServiceStateMapUpdateRequest.create(null, keysToRemove);
        patchOperation = Operation.createPatch(new URI("http://test")).setBody(request);
        result = Utils.mergeWithStateAdvanced(desc, state, MergeTest.class, patchOperation);
        assertTrue(result.contains(MergeResult.SPECIAL_MERGE));
        assertTrue(result.contains(MergeResult.STATE_CHANGED));
        assertEquals(state.mapOfStrings.size(), 1);
        assertTrue(state.mapOfStrings.containsKey("key-2"));
        assertFalse(state.mapOfStrings.containsKey("key-1"));

        // repeating the update should not change the state anymore
        result = Utils.mergeWithStateAdvanced(desc, state, MergeTest.class, patchOperation);
        assertTrue(result.contains(MergeResult.SPECIAL_MERGE));
        assertFalse(result.contains(MergeResult.STATE_CHANGED));
    }

    @Test
    public void testMergeWithStateAdvanced() throws Throwable {
        MergeTest state = new MergeTest();
        state.s = "one";

        MergeTest patch = new MergeTest();
        patch.s = "two";

        Operation patchOperation = Operation.createPatch(new URI("http://test")).setBody(patch);
        ServiceDocumentDescription desc = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        EnumSet<MergeResult> result = Utils.mergeWithStateAdvanced(desc, state, MergeTest.class,
                patchOperation);
        assertFalse(result.contains(MergeResult.SPECIAL_MERGE));
        assertTrue(result.contains(MergeResult.STATE_CHANGED));
        assertEquals("two", state.s);

        // repeating the update should not change the state anymore
        result = Utils.mergeWithStateAdvanced(desc, state, MergeTest.class, patchOperation);
        assertFalse(result.contains(MergeResult.SPECIAL_MERGE));
        assertFalse(result.contains(MergeResult.STATE_CHANGED));
        assertEquals("two", state.s);
    }

    @Test
    public void testSerializeClassesWithoutDefaultConstructor() {
        Range range = new Range(0, 100);
        // clone uses kryo serialization
        Range clone = Utils.clone(range);
        assertEquals(range.from, clone.from);
        assertEquals(range.to, clone.to);
    }

    /**
     * Test merging partially defined patch object.
     */
    @Test
    public void testPartialMerge() {
        MergeTest source = new MergeTest();
        source.s = SOME_STRING_VALUE;
        source.x = SOME_INT_VALUE;
        source.ignore = SOME_IGNORE_VALUE;
        MergeTest patch = new MergeTest();
        patch.x = SOME_OTHER_INT_VALUE;
        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertTrue("There should be changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_OTHER_INT_VALUE);
        Assert.assertEquals("Non-annotated ignore field", source.ignore, SOME_IGNORE_VALUE);
    }

    /**
     * Test merging in an empty patch.
     */
    @Test
    public void testEmptyPatch() {
        MergeTest source = new MergeTest();
        source.s = SOME_STRING_VALUE;
        source.x = SOME_INT_VALUE;
        source.ignore = SOME_IGNORE_VALUE;
        MergeTest patch = new MergeTest();
        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertFalse("There should be no changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_INT_VALUE);
        Assert.assertEquals("Non-annotated ignore field", source.ignore, SOME_IGNORE_VALUE);
    }

    /**
     * Test merging in an empty patch over a non-empty source.
     */
    @Test
    public void testEmptyPatchNonEmptySource() {
        MergeTest source = new MergeTest();
        source.s = SOME_STRING_VALUE;
        source.x = SOME_INT_VALUE;
        source.ignore = SOME_IGNORE_VALUE;
        source.intArray = new int[] { SOME_INT_VALUE };
        source.listOfStrings = Arrays.asList(SOME_STRING_VALUE, SOME_OTHER_STRING_VALUE);
        source.mapOfStrings = new HashMap<String, String>();
        source.mapOfStrings.put(SOME_STRING_VALUE, SOME_STRING_VALUE);
        source.setOfStrings = new HashSet<String>();
        source.setOfStrings.add(SOME_STRING_VALUE);
        MergeTest patch = new MergeTest();
        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertFalse("There should be no changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_INT_VALUE);
        Assert.assertEquals("Non-annotated ignore field", source.ignore, SOME_IGNORE_VALUE);
    }

    /**
     * Test merging in an empty patch over a non-empty source.
     */
    @Test
    public void testEmptyPatchCollectionsNonEmptySource() {
        MergeTest source = new MergeTest();
        source.s = SOME_STRING_VALUE;
        source.x = SOME_INT_VALUE;
        source.ignore = SOME_IGNORE_VALUE;
        source.intArray = new int[] { SOME_INT_VALUE };
        source.listOfStrings = Arrays.asList(SOME_STRING_VALUE, SOME_OTHER_STRING_VALUE);
        source.mapOfStrings = new HashMap<String, String>();
        source.mapOfStrings.put(SOME_STRING_VALUE, SOME_STRING_VALUE);
        source.setOfStrings = new HashSet<String>();
        source.setOfStrings.add(SOME_STRING_VALUE);
        MergeTest patch = new MergeTest();
        patch.listOfStrings = new ArrayList<>();
        patch.setOfStrings = new HashSet<>();
        patch.mapOfStrings = new HashMap<>();
        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertFalse("There should be no changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_INT_VALUE);
        Assert.assertEquals("Non-annotated ignore field", source.ignore, SOME_IGNORE_VALUE);
    }

    /**
     * Test merging patch with same values as existing value.
     */
    @Test
    public void testEqualsMerge() {
        MergeTest source = new MergeTest();
        source.s = SOME_STRING_VALUE;
        source.x = SOME_INT_VALUE;
        source.ignore = SOME_IGNORE_VALUE;
        MergeTest patch = new MergeTest();
        patch.s = source.s;
        patch.x = source.x;
        patch.ignore = source.ignore;
        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertFalse("There should be no changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_INT_VALUE);
        Assert.assertEquals("Non-annotated ignore field", source.ignore, SOME_IGNORE_VALUE);
    }

    /**
     * Test merging patch with same values as existing value.
     */
    @Test
    public void testEqualsMergeNonEmptySource() {
        MergeTest source = new MergeTest();
        source.s = SOME_STRING_VALUE;
        source.x = SOME_INT_VALUE;
        source.ignore = SOME_IGNORE_VALUE;
        source.intArray = new int[] { SOME_INT_VALUE };
        source.listOfStrings = new ArrayList<>();
        source.listOfStrings.add(SOME_STRING_VALUE);
        source.listOfStrings.add(SOME_OTHER_STRING_VALUE);
        source.mapOfStrings = new HashMap<String, String>();
        source.mapOfStrings.put(SOME_STRING_VALUE, SOME_STRING_VALUE);
        source.setOfStrings = new HashSet<String>();
        source.setOfStrings.add(SOME_STRING_VALUE);

        MergeTest patch = new MergeTest();
        patch.s = source.s;
        patch.x = source.x;
        patch.ignore = source.ignore;
        patch.mapOfStrings = source.mapOfStrings;
        patch.setOfStrings = source.setOfStrings;

        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertFalse("There should be no changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_INT_VALUE);
        Assert.assertEquals("Non-annotated ignore field", source.ignore, SOME_IGNORE_VALUE);
    }

    @Test
    public void testComputeSignatureChanged() {
        ServiceDocumentDescription description = ServiceDocumentDescription.Builder.create()
                .buildDescription(QueryValidationTestService.QueryValidationServiceState.class);

        QueryValidationTestService.QueryValidationServiceState document = new QueryValidationTestService.QueryValidationServiceState();
        document.documentSelfLink = "testComputeSignatureChange";
        document.textValue = "valueA";
        document.documentExpirationTimeMicros = 1;
        String initialSignature = Utils.computeSignature(document, description);

        document.textValue = "valueB";
        String valueChangedSignature = Utils.computeSignature(document, description);

        assertNotEquals(initialSignature, valueChangedSignature);

        document.documentExpirationTimeMicros = 2;
        String expirationChangedSignature = Utils.computeSignature(document, description);

        assertNotEquals(initialSignature, expirationChangedSignature);
        assertNotEquals(valueChangedSignature, expirationChangedSignature);
    }

    @Test
    public void testComputeSignatureUnchanged() {
        ServiceDocumentDescription description = ServiceDocumentDescription.Builder.create()
                .buildDescription(QueryValidationTestService.QueryValidationServiceState.class);

        QueryValidationTestService.QueryValidationServiceState document = new QueryValidationTestService.QueryValidationServiceState();
        document.documentSelfLink = "testComputeSignatureChange";
        document.textValue = "valueA";
        document.documentUpdateTimeMicros = 1;
        String initialSignature = Utils.computeSignature(document, description);

        document.documentUpdateTimeMicros = 2;
        String updateChangedSignature = Utils.computeSignature(document, description);

        assertEquals(initialSignature, updateChangedSignature);
    }

    /**
     * Test service document.
     */
    private static class MergeTest extends ServiceDocument {
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Integer x;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.INFRASTRUCTURE)
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String s;
        public String ignore;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public List<String> listOfStrings;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Set<String> setOfStrings;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Map<String, String> mapOfStrings;
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public int[] intArray;
    }

    @ServiceDocument.IndexingParameters(serializedStateSize = 8, versionRetention = 44,
            versionRetentionFloor = 22)
    @ServiceDocument.Documentation(name = "Test Document Name", description = "Test Document Desc")
    private static class AnnotatedDoc extends ServiceDocument {
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        @Documentation(description = "desc", exampleString = "example")
        public String opt;

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.ID)
        @PropertyOptions(
                indexing = {
                    ServiceDocumentDescription.PropertyIndexingOption.SORT,
                    ServiceDocumentDescription.PropertyIndexingOption.EXCLUDE_FROM_SIGNATURE},
                usage = {
                    ServiceDocumentDescription.PropertyUsageOption.OPTIONAL})
        public String opts;

        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.EXPAND)
        public Range nestedPodo;

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.OPTIONAL)
        public RoundingMode someEnum;

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.OPTIONAL)
        public Enum<?> justEnum;

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.ID)
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.REQUIRED)
        public String requiredId;

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.REQUIRED)
        public String required;
    }

    @Test
    public void testAnnotationOnFields() {
        ServiceDocumentDescription.Builder builder = ServiceDocumentDescription.Builder.create();
        ServiceDocumentDescription desc = builder.buildDescription(AnnotatedDoc.class);
        assertEquals("Test Document Name", desc.name);
        assertEquals("Test Document Desc", desc.description);
        assertEquals(8, desc.serializedStateSizeLimit);
        assertEquals(44, desc.versionRetentionLimit);
        assertEquals(22, desc.versionRetentionFloor);

        ServiceDocumentDescription.PropertyDescription optDesc = desc.propertyDescriptions
                .get("opt");
        assertEquals(optDesc.usageOptions,
                EnumSet.of(ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL));
        assertEquals(optDesc.indexingOptions,
                EnumSet.of(ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY));
        assertEquals(optDesc.exampleValue, "example");
        assertEquals(optDesc.propertyDocumentation, "desc");

        ServiceDocumentDescription.PropertyDescription optsDesc = desc.propertyDescriptions
                .get("opts");
        assertEquals(optsDesc.usageOptions,
                EnumSet.of(ServiceDocumentDescription.PropertyUsageOption.ID,
                        ServiceDocumentDescription.PropertyUsageOption.OPTIONAL));
        assertEquals(optsDesc.indexingOptions,
                EnumSet.of(ServiceDocumentDescription.PropertyIndexingOption.SORT,
                        ServiceDocumentDescription.PropertyIndexingOption.EXCLUDE_FROM_SIGNATURE));

        ServiceDocumentDescription.PropertyDescription requiredIdDesc = desc.propertyDescriptions
                .get("requiredId");
        assertEquals(requiredIdDesc.usageOptions,
                EnumSet.of(ServiceDocumentDescription.PropertyUsageOption.REQUIRED,
                        ServiceDocumentDescription.PropertyUsageOption.ID));

        ServiceDocumentDescription.PropertyDescription requiredDesc = desc.propertyDescriptions
                .get("required");
        assertEquals(requiredDesc.usageOptions,
                EnumSet.of(ServiceDocumentDescription.PropertyUsageOption.REQUIRED));
    }

    @Test
    public void testNestedPodosAreAssignedKinds() {
        ServiceDocumentDescription desc = ServiceDocumentDescription.Builder.create()
                .buildDescription(AnnotatedDoc.class);
        ServiceDocumentDescription.PropertyDescription nestedPodo = desc.propertyDescriptions.get("nestedPodo");
        assertEquals(Utils.buildKind(Range.class), nestedPodo.kind);

        // primitives don't have a kind
        ServiceDocumentDescription.PropertyDescription opt = desc.propertyDescriptions.get("opt");
        assertNull(opt.kind);
    }

    @Test
    public void testEnumValuesArePopulated() {
        ServiceDocumentDescription desc = ServiceDocumentDescription.Builder.create()
                .buildDescription(AnnotatedDoc.class);
        ServiceDocumentDescription.PropertyDescription someEnum = desc.propertyDescriptions.get("someEnum");
        ServiceDocumentDescription.PropertyDescription nestedPodo = desc.propertyDescriptions.get("nestedPodo");
        ServiceDocumentDescription.PropertyDescription justEnum = desc.propertyDescriptions.get("justEnum");

        assertEquals(RoundingMode.values().length, someEnum.enumValues.length);
        assertNull(nestedPodo.enumValues);

        // handle generic classes where the type parameter is Enum
        assertNull(justEnum.enumValues);
    }

    @Test
    public void testNumberFieldsCoercedToDouble() {
        ServiceDocumentDescription.PropertyDescription desc = ServiceDocumentDescription.Builder
                .create()
                .buildPodoPropertyDescription(QueryTask.NumericRange.class);
        assertEquals(ServiceDocumentDescription.TypeName.DOUBLE, desc.fieldDescriptions.get("min").typeName);
        assertEquals(ServiceDocumentDescription.TypeName.DOUBLE, desc.fieldDescriptions.get("max").typeName);
    }

    /**
     * Create a ServiceDocument instance with fields of all known types.
     */
    public static class MultiTypeServiceDocument extends ServiceDocument {

        public boolean bo;
        public int i;
        public byte b;
        public char c;
        public short s;
        public long l;
        public float f;
        public double d;

        public Boolean aBoolean;
        public Byte aByte;
        public Character aCharacter;
        public Integer anInteger;
        public Short aShort;
        public Long aLong;
        public Float aFloat;
        public Double aDouble;
        public Number aNumber;
        public BigInteger bigInteger;
        public BigDecimal bigDecimal;

        public Void aVoid;

        public Date aData;
        public byte[] byteArray;
        public char[] characterArray;
        public CharSequence charSequence;

        public AtomicLong anAtomicLong;
    }

   /**
    * Verify that the document builder builds valid example values for all
    * known types.
    *
    * This functionality is used in StatefulService.getDocumentTemplate()
    *
    * @throws Throwable if the exampleValues could not be assigned to an empty
    *                   ServiceDocument instance
    */
    @Test
    public void exampleValues() throws Throwable {
        MultiTypeServiceDocument doc = new MultiTypeServiceDocument();

        ServiceDocumentDescription desc = ServiceDocumentDescription.Builder.create()
                .buildDescription(MultiTypeServiceDocument.class);
        assertNotNull(desc);

        for (ServiceDocumentDescription.PropertyDescription pd : desc.propertyDescriptions.values()) {
            pd.accessor.set(doc, pd.exampleValue);
        }
    }
}
