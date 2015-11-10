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

package com.vmware.dcp.services.common;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceDocumentDescription;
import com.vmware.dcp.common.TestUtils;

public class TestServiceDocument {

    @Test
    public void testServiceDocumentEquality() throws Throwable {
        ServiceDocumentDescription description = TestUtils.buildStateDescription(
                ExampleService.ExampleServiceState.class, null);
        ExampleService.ExampleServiceState initialState = new ExampleService.ExampleServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = 5L;

        ExampleService.ExampleServiceState modifiedState = new ExampleService
                .ExampleServiceState();
        modifiedState.name = initialState.name;
        modifiedState.counter = initialState.counter;

        boolean value = ServiceDocument.equals(description, initialState, modifiedState);
        assertEquals(true, value);
    }

    @Test
    public void testServiceDocumentNonEquality() throws Throwable {
        ServiceDocumentDescription description = TestUtils.buildStateDescription(
                ExampleService.ExampleServiceState.class, null);
        ExampleService.ExampleServiceState initialState = new ExampleService.ExampleServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = 5L;

        ExampleService.ExampleServiceState modifiedState = new ExampleService
                .ExampleServiceState();
        modifiedState.name = initialState.name;
        modifiedState.counter = 10L;

        boolean value = ServiceDocument.equals(description, initialState, modifiedState);
        assertEquals(false, value);
    }

    @Test
    public void testServiceDocumentCoreProperties() throws Throwable {
        ServiceDocumentDescription description = TestUtils.buildStateDescription(
                ExampleService.ExampleServiceState.class, null);
        ExampleService.ExampleServiceState initialState = new ExampleService.ExampleServiceState();
        initialState.documentOwner = UUID.randomUUID().toString();
        ExampleService.ExampleServiceState modifiedState = new ExampleService
                .ExampleServiceState();
        modifiedState.documentOwner = UUID.randomUUID().toString();

        boolean value = ServiceDocument.equals(description, initialState, modifiedState);
        assertEquals(true, value);
    }
}
