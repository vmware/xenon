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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Date;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryValidationTestService;

public class TestServiceDocument {

    public static final Integer SOME_INT_VALUE = 100;
    public static final Integer SOME_OTHER_INT_VALUE = 200;
    public static final long SOME_EXPIRATION_VALUE = Utils.getNowMicrosUtc();
    public static final String SOME_STRING_VALUE = "some value";
    public static final String SOME_OTHER_STRING_VALUE = "some other value";
    public static final String SOME_IGNORE_VALUE = "ignore me";
    public static final String SOME_OTHER_IGNORE_VALUE = "ignore me please";
    public static final long SOME_OTHER_EXPIRATION_VALUE =
            Utils.getNowMicrosUtc() + TimeUnit.MINUTES.toMicros(5);

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
        MergeTest patch = new MergeTest();
        patch.s = SOME_OTHER_STRING_VALUE;
        patch.x = SOME_OTHER_INT_VALUE;
        patch.ignore = SOME_OTHER_IGNORE_VALUE;
        patch.documentExpirationTimeMicros = SOME_OTHER_EXPIRATION_VALUE;

        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertTrue("There should be changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_OTHER_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_OTHER_INT_VALUE);
        Assert.assertEquals("Non-annotated ignore field", source.ignore, SOME_IGNORE_VALUE);
        Assert.assertEquals("Auto-annotated expiration field", source.documentExpirationTimeMicros,
                SOME_OTHER_EXPIRATION_VALUE);
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
        patch.ignore = SOME_OTHER_IGNORE_VALUE;
        ServiceDocumentDescription d = ServiceDocumentDescription.Builder.create()
                .buildDescription(MergeTest.class);
        Assert.assertTrue("There should be changes", Utils.mergeWithState(d, source, patch));
        Assert.assertEquals("Annotated s field", source.s, SOME_OTHER_STRING_VALUE);
        Assert.assertEquals("Annotated x field", source.x, SOME_OTHER_INT_VALUE);
        Assert.assertNull("Non-annotated ignore field", source.ignore);
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
    public void testEmptyMerge() {
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
        patch.ignore = SOME_OTHER_IGNORE_VALUE;
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
        document.stringValue = "valueA";
        document.documentExpirationTimeMicros = 1;
        String initialSignature = Utils.computeSignature(document, description);

        document.stringValue = "valueB";
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
        document.stringValue = "valueA";
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
    }


    @ServiceDocument.IndexingParameters(serializedStateSize = 8, versionRetention = 44)
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
    }

    @Test
    public void testAnnotationOnFields() {
        ServiceDocumentDescription.Builder builder = ServiceDocumentDescription.Builder.create();
        ServiceDocumentDescription desc = builder.buildDescription(AnnotatedDoc.class);
        assertEquals(8, desc.serializedStateSizeLimit);
        assertEquals(44, desc.versionRetentionLimit);

        ServiceDocumentDescription.PropertyDescription optDesc = desc.propertyDescriptions.get("opt");
        assertEquals(optDesc.usageOptions, EnumSet.of(ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL));
        assertEquals(optDesc.indexingOptions, EnumSet.of(ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY));
        assertEquals(optDesc.exampleValue, "example");
        assertEquals(optDesc.propertyDocumentation, "desc");

        ServiceDocumentDescription.PropertyDescription optsDesc = desc.propertyDescriptions.get("opts");
        assertEquals(optsDesc.usageOptions, EnumSet.of(ServiceDocumentDescription.PropertyUsageOption.ID, ServiceDocumentDescription.PropertyUsageOption.OPTIONAL));
        assertEquals(optsDesc.indexingOptions, EnumSet.of(ServiceDocumentDescription.PropertyIndexingOption.SORT, ServiceDocumentDescription.PropertyIndexingOption.EXCLUDE_FROM_SIGNATURE));
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
