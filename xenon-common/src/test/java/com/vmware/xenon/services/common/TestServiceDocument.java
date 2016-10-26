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

import java.util.EnumSet;
import java.util.UUID;

import org.junit.Test;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocument.DocumentRelationship;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.TestUtils;
import com.vmware.xenon.common.Utils;

public class TestServiceDocument {

    @Test
    public void copyTo() throws Throwable {
        // if a field is added to ServiceDocument, this method must be updated.
        // Also, methods ServiceDocument.isBuiltInFieldXXX() must be updated as well
        assertEquals(27, ServiceDocument.class.getFields().length);
        ServiceDocument one = new ServiceDocument();
        one.documentAuthPrincipalLink = UUID.randomUUID().toString();
        one.documentDescription = null;
        one.documentEpoch = Utils.getNowMicrosUtc1();
        one.documentExpirationTimeMicros = Utils.getNowMicrosUtc1();
        one.documentKind = UUID.randomUUID().toString();
        one.documentOwner = UUID.randomUUID().toString();
        one.documentSelfLink = UUID.randomUUID().toString();
        one.documentSourceLink = UUID.randomUUID().toString();
        one.documentTransactionId = UUID.randomUUID().toString();
        one.documentUpdateAction = UUID.randomUUID().toString();
        one.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();
        one.documentVersion = Utils.getNowMicrosUtc1();

        ServiceDocument two = new ServiceDocument();
        one.copyTo(two);

        assertEquals(one.documentAuthPrincipalLink, two.documentAuthPrincipalLink);
        assertEquals(one.documentDescription, two.documentDescription);
        assertEquals(one.documentEpoch, two.documentEpoch);
        assertEquals(one.documentExpirationTimeMicros, two.documentExpirationTimeMicros);
        assertEquals(one.documentKind, two.documentKind);
        assertEquals(one.documentOwner, two.documentOwner);
        assertEquals(one.documentSelfLink, two.documentSelfLink);
        assertEquals(one.documentSourceLink, two.documentSourceLink);
        assertEquals(one.documentTransactionId, two.documentTransactionId);
        assertEquals(one.documentUpdateAction, two.documentUpdateAction);
        assertEquals(one.documentUpdateTimeMicros, two.documentUpdateTimeMicros);
        assertEquals(one.documentVersion, two.documentVersion);
    }

    @Test
    public void equals() throws Throwable {
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

        initialState = new ExampleService.ExampleServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = 5L;

        modifiedState = new ExampleService
                .ExampleServiceState();
        modifiedState.name = initialState.name;
        modifiedState.counter = 10L;

        value = ServiceDocument.equals(description, initialState, modifiedState);
        assertEquals(false, value);

        // set a core document field to be different between states and still verify
        // the states compare as equals (core fields are ignored)
        initialState = new ExampleService.ExampleServiceState();
        initialState.documentOwner = UUID.randomUUID().toString();
        initialState.counter = 10L;

        modifiedState = new ExampleService
                .ExampleServiceState();
        modifiedState.documentOwner = UUID.randomUUID().toString();
        modifiedState.counter = 10L;

        value = ServiceDocument.equals(description, initialState, modifiedState);
        assertEquals(true, value);
    }

    @Test
    public void compare() throws Throwable {
        ServiceDocumentDescription description = TestUtils.buildStateDescription(
                ExampleService.ExampleServiceState.class, null);

        ExampleService.ExampleServiceState stateA = new ExampleService.ExampleServiceState();
        ExampleService.ExampleServiceState stateB = new ExampleService.ExampleServiceState();

        stateA.name = UUID.randomUUID().toString();
        stateB.name = stateA.name;

        // different versions, equal time, B should be preferred
        stateA.documentVersion = 1;
        stateB.documentVersion = 2;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();
        stateB.documentUpdateTimeMicros = stateA.documentUpdateTimeMicros;

        EnumSet<DocumentRelationship> results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // equal versions, equal time, neither is preferred
        stateA.documentVersion = 1;
        stateB.documentVersion = 1;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();
        stateB.documentUpdateTimeMicros = stateA.documentUpdateTimeMicros;

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(results.contains(DocumentRelationship.EQUAL_VERSION));
        assertTrue(results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // A higher version, older time, A is preferred
        stateA.documentVersion = 10;
        stateB.documentVersion = 1;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc1()
                + Utils.getTimeComparisonEpsilonMicros() * 2;

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(!results.contains(DocumentRelationship.EQUAL_VERSION));
        assertTrue(!results.contains(DocumentRelationship.NEWER_UPDATE_TIME));
        assertTrue(!results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // equal versions, higher time outside epsilon, A is preferred
        stateA.documentVersion = 1;
        stateB.documentVersion = 1;
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc1()
                + Utils.getTimeComparisonEpsilonMicros() * 2;

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(results.contains(DocumentRelationship.EQUAL_VERSION));
        assertTrue(results.contains(DocumentRelationship.NEWER_UPDATE_TIME));
        assertTrue(!results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // equal versions, older time outside epsilon, A is NOT preferred
        stateA.documentVersion = 1;
        stateB.documentVersion = 1;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc1()
                + Utils.getTimeComparisonEpsilonMicros() * 2;

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(results.contains(DocumentRelationship.EQUAL_VERSION));
        assertTrue(!results.contains(DocumentRelationship.NEWER_UPDATE_TIME));
        assertTrue(!results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // equal versions, time within epsilon, states equal, no conflict
        stateA.documentVersion = 1;
        stateB.documentVersion = 1;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc1()
                + Utils.getTimeComparisonEpsilonMicros() / 2;

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(results.contains(DocumentRelationship.EQUAL_VERSION));
        assertTrue(!results.contains(DocumentRelationship.NEWER_UPDATE_TIME));
        assertTrue(!results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // equal versions, time within epsilon (B newer), states NOT equal, in conflict
        stateB.counter = Long.MAX_VALUE;
        stateA.documentVersion = 1;
        stateB.documentVersion = 1;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc1()
                + Utils.getTimeComparisonEpsilonMicros() / 2;

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(results.contains(DocumentRelationship.EQUAL_VERSION));
        assertTrue(!results.contains(DocumentRelationship.NEWER_UPDATE_TIME));
        assertTrue(!results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(results.contains(DocumentRelationship.IN_CONFLICT));

        // equal versions, time within epsilon (A newer), states NOT equal, in conflict
        stateB.counter = Long.MAX_VALUE;
        stateA.documentVersion = 1;
        stateB.documentVersion = 1;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc1()
                + Utils.getTimeComparisonEpsilonMicros() / 2;
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc1();

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(results.contains(DocumentRelationship.EQUAL_VERSION));
        assertTrue(results.contains(DocumentRelationship.NEWER_UPDATE_TIME));
        assertTrue(!results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(results.contains(DocumentRelationship.IN_CONFLICT));

    }
}
