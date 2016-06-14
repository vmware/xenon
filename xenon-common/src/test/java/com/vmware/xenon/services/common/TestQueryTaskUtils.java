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

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestQueryTaskUtils {

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
