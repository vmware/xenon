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

package com.vmware.xenon.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

public class TestQueryTaskUtils {

    @Test
    public void testMergeQueryResultsWithSameData() {

        ServiceDocumentQueryResult result1 = createServiceDocumentQueryResult(
                new int[] { 1, 10, 2, 3, 4, 5, 6, 7, 8, 9 });
        ServiceDocumentQueryResult result2 = createServiceDocumentQueryResult(
                new int[] { 1, 10, 2, 3, 4, 5, 6, 7, 8, 9 });
        ServiceDocumentQueryResult result3 = createServiceDocumentQueryResult(
                new int[] { 1, 10, 2, 3, 4, 5, 6, 7, 8, 9 });

        List<ServiceDocumentQueryResult> resultsToMerge = Arrays.asList(result1, result2, result3);
        ServiceDocumentQueryResult mergeResult = new ServiceDocumentQueryResult();
        QueryTaskUtils.mergeQueryResults(resultsToMerge, true, mergeResult);

        assertTrue(verifyMergeResult(mergeResult, new int[] { 1, 10, 2, 3, 4, 5, 6, 7, 8, 9 }));
    }

    @Test
    public void testMergeQueryResultsWithDifferentData() {

        ServiceDocumentQueryResult result1 = createServiceDocumentQueryResult(
                new int[] { 1, 3, 4, 5, 7, 9 });
        ServiceDocumentQueryResult result2 = createServiceDocumentQueryResult(
                new int[] { 10, 2, 3, 4, 5, 6, 9 });
        ServiceDocumentQueryResult result3 = createServiceDocumentQueryResult(
                new int[] { 1, 10, 2, 3, 4, 8 });

        List<ServiceDocumentQueryResult> resultsToMerge = Arrays.asList(result1, result2, result3);
        ServiceDocumentQueryResult mergeResult = new ServiceDocumentQueryResult();
        QueryTaskUtils.mergeQueryResults(resultsToMerge, true, mergeResult);

        assertTrue(verifyMergeResult(mergeResult, new int[] { 1, 10, 2, 3, 4, 5, 6, 7, 8, 9 }));
    }

    @Test
    public void testMergeQueryResultsWithEmptySet() {

        ServiceDocumentQueryResult result1 = createServiceDocumentQueryResult(
                new int[] { 1, 3, 4, 5, 7, 8, 9 });
        ServiceDocumentQueryResult result2 = createServiceDocumentQueryResult(
                new int[] { 10, 2, 3, 4, 5, 6, 9 });
        ServiceDocumentQueryResult result3 = createServiceDocumentQueryResult(new int[] {});

        List<ServiceDocumentQueryResult> resultsToMerge = Arrays.asList(result1, result2, result3);
        ServiceDocumentQueryResult mergeResult = new ServiceDocumentQueryResult();
        QueryTaskUtils.mergeQueryResults(resultsToMerge, true, mergeResult);

        assertTrue(verifyMergeResult(mergeResult, new int[] { 1, 10, 2, 3, 4, 5, 6, 7, 8, 9 }));
    }

    @Test
    public void testMergeQueryResultsWithAllEmpty() {

        ServiceDocumentQueryResult result1 = createServiceDocumentQueryResult(new int[] {});
        ServiceDocumentQueryResult result2 = createServiceDocumentQueryResult(new int[] {});
        ServiceDocumentQueryResult result3 = createServiceDocumentQueryResult(new int[] {});

        List<ServiceDocumentQueryResult> resultsToMerge = Arrays.asList(result1, result2, result3);
        ServiceDocumentQueryResult mergeResult = new ServiceDocumentQueryResult();
        QueryTaskUtils.mergeQueryResults(resultsToMerge, true, mergeResult);

        assertTrue(verifyMergeResult(mergeResult, new int[] {}));
    }

    @Test
    public void testMergeQueryResultsInDescOrder() {
        ServiceDocumentQueryResult result1 = createServiceDocumentQueryResult(
                new int[] { 9, 7, 5, 4, 3, 1 });
        ServiceDocumentQueryResult result2 = createServiceDocumentQueryResult(
                new int[] { 9, 6, 5, 4, 3, 2, 10 });
        ServiceDocumentQueryResult result3 = createServiceDocumentQueryResult(
                new int[] { 8, 4, 3, 2, 10, 1 });

        List<ServiceDocumentQueryResult> resultsToMerge = Arrays.asList(result1, result2, result3);
        ServiceDocumentQueryResult mergeResult = new ServiceDocumentQueryResult();
        QueryTaskUtils.mergeQueryResults(resultsToMerge, false, mergeResult);

        assertTrue(verifyMergeResult(mergeResult, new int[] { 9, 8, 7, 6, 5, 4, 3, 2, 10, 1 }));
    }

    @Test
    public void testMergeQueryResultsWhenCountOptions() {
        ServiceDocumentQueryResult result1 = createServiceDocumentQueryResult(
                new int[] { 9, 7, 5, 4, 3, 1 });
        result1.documentLinks.clear();
        result1.documents.clear();
        ServiceDocumentQueryResult result2 = createServiceDocumentQueryResult(
                new int[] { 9, 6, 5, 4, 3, 2, 10 });
        result2.documentLinks.clear();
        result2.documents.clear();
        ServiceDocumentQueryResult result3 = createServiceDocumentQueryResult(
                new int[] { 8, 4, 3, 2, 10, 1 });
        result3.documentLinks.clear();
        result3.documents.clear();

        List<ServiceDocumentQueryResult> resultsToMerge = Arrays.asList(result1, result2, result3);
        ServiceDocumentQueryResult mergeResult = new ServiceDocumentQueryResult();
        QueryTaskUtils.mergeQueryResults(resultsToMerge,
                false,
                EnumSet.of(QueryOption.COUNT), mergeResult);

        assertEquals(result2.documentCount, mergeResult.documentCount);
    }

    private ServiceDocumentQueryResult createServiceDocumentQueryResult(int[] documentIndices) {

        ServiceDocumentQueryResult result = new ServiceDocumentQueryResult();
        result.documentCount = (long) documentIndices.length;
        result.documents = new HashMap<>();

        for (int index : documentIndices) {
            String documentLink = ServiceUriPaths.CORE_LOCAL_QUERY_TASKS + "/document" + index;
            result.documentLinks.add(documentLink);
            result.documents.put(documentLink, new Object());
        }

        return result;
    }

    private boolean verifyMergeResult(ServiceDocumentQueryResult mergeResult,
            int[] expectedSequence) {
        if (mergeResult.documentCount != expectedSequence.length) {
            return false;
        }

        for (int i = 0; i < expectedSequence.length; i++) {
            String expectedLink = ServiceUriPaths.CORE_LOCAL_QUERY_TASKS + "/document"
                    + expectedSequence[i];
            if (!expectedLink.equals(mergeResult.documentLinks.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testGetQueryPropertyNames() throws Throwable {
        ServiceDocumentDescription desc = Builder.create().buildDescription(
                ExampleServiceState.class);
        Set<String> queryPropertyNames = QueryTaskUtils.getExpandedQueryPropertyNames(desc);

        Set<String> expectedPropertyNames = new HashSet<>();
        expectedPropertyNames.add(ExampleServiceState.FIELD_NAME_COUNTER);
        expectedPropertyNames.add(ExampleServiceState.FIELD_NAME_SORTED_COUNTER);
        expectedPropertyNames.add(ExampleServiceState.FIELD_NAME_NAME);
        expectedPropertyNames.add(ExampleServiceState.FIELD_NAME_ID);
        expectedPropertyNames.add(ExampleServiceState.FIELD_NAME_REQUIRED);
        expectedPropertyNames.add(ExampleServiceState.FIELD_NAME_KEY_VALUES);
        expectedPropertyNames.add(ExampleServiceState.FIELD_NAME_IS_FROM_MIGRATION);
        expectedPropertyNames.add("tags.item");

        assertEquals(expectedPropertyNames, queryPropertyNames);
    }

    @Test
    public void testGetMultiLevelNestedQueryPropertyNames() throws Throwable {
        ServiceDocumentDescription desc = Builder.create().buildDescription(
                ComplexServiceState.class);
        Set<String> queryPropertyNames = QueryTaskUtils.getExpandedQueryPropertyNames(desc);

        Set<String> expectedPropertyNames = new HashSet<>();
        expectedPropertyNames.add("name");

        expectedPropertyNames.add("innerServices.item.nameInner");

        expectedPropertyNames.add("singleInnerService.nameInner");
        expectedPropertyNames.add("singleInnerService.singleInnerService.nameSecondInner");

        assertEquals(expectedPropertyNames, queryPropertyNames);
    }

    public static class ComplexServiceState extends ServiceDocument {
        public String name;
        @PropertyOptions(indexing = PropertyIndexingOption.EXPAND)
        public ComplexInnerServiceState singleInnerService;
        @PropertyOptions(indexing = PropertyIndexingOption.EXPAND)
        public Set<ComplexInnerServiceState> innerServices;
    }

    public static class ComplexInnerServiceState extends ServiceDocument {
        public String nameInner;
        @PropertyOptions(indexing = PropertyIndexingOption.EXPAND)
        public ComplexSecondInnerServiceState singleInnerService;
        @PropertyOptions(indexing = PropertyIndexingOption.EXPAND)
        public Set<ComplexSecondInnerServiceState> innerServices;
    }

    public static class ComplexSecondInnerServiceState extends ServiceDocument {
        public String nameSecondInner;
        @PropertyOptions(indexing = PropertyIndexingOption.EXPAND)
        public ComplexThirdInnerServiceState singleInnerService;
        @PropertyOptions(indexing = PropertyIndexingOption.EXPAND)
        public Set<ComplexThirdInnerServiceState> innerServices;
    }

    public static class ComplexThirdInnerServiceState extends ServiceDocument {
        public String nameThirdInner;
    }
}
