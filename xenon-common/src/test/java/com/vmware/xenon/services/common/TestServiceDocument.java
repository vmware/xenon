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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
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
        one.documentEpoch = Utils.getNowMicrosUtc();
        one.documentExpirationTimeMicros = Utils.getNowMicrosUtc();
        one.documentKind = UUID.randomUUID().toString();
        one.documentOwner = UUID.randomUUID().toString();
        one.documentSelfLink = UUID.randomUUID().toString();
        one.documentSourceLink = UUID.randomUUID().toString();
        one.documentTransactionId = UUID.randomUUID().toString();
        one.documentUpdateAction = UUID.randomUUID().toString();
        one.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        one.documentVersion = Utils.getNowMicrosUtc();

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

    public static class ComparableServiceState extends ExampleService.ExampleServiceState {
        public Set<String> collectionValue;
        public Boolean booleanValue;
    }

    public static class DatastoreServiceState extends ServiceDocument {

        public static final String FIELD_NAME_TAGS = "tags";
        public static final String FIELD_NAME_ID = "id";
        public static final String FIELD_NAME_TYPE = "type";

        public String id;
        public String name;
        public String type;
        public Set<String> tags;
        public Boolean isImageDatastore;
    }

    @Test
    public void equals() throws Throwable {
        ServiceDocumentDescription description = TestUtils.buildStateDescription(
                ComparableServiceState.class, null);
        ComparableServiceState initialState = new ComparableServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = 5L;
        initialState.booleanValue = false;

        ComparableServiceState modifiedState = new ComparableServiceState();
        modifiedState.name = initialState.name;
        modifiedState.counter = initialState.counter;
        modifiedState.booleanValue = initialState.booleanValue;

        boolean value = ServiceDocument.equals(description, initialState, modifiedState);
        assertTrue(value);

        initialState = new ComparableServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = 5L;
        initialState.booleanValue = false;

        modifiedState = new ComparableServiceState();
        modifiedState.name = initialState.name;
        modifiedState.counter = 10L;
        modifiedState.booleanValue = initialState.booleanValue;

        value = ServiceDocument.equals(description, initialState, modifiedState);
        assertFalse(value);

        initialState = new ComparableServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = 5L;
        initialState.collectionValue = new HashSet<>(Collections.singleton("String 1"));
        initialState.booleanValue = false;

        modifiedState = new ComparableServiceState();
        modifiedState.name = initialState.name;
        modifiedState.counter = initialState.counter;
        modifiedState.collectionValue = new HashSet<>(Collections.singleton("String 1"));
        modifiedState.booleanValue = true;

        value = ServiceDocument.equals(description, initialState, modifiedState);
        assertFalse(value);

        // set a core document field to be different between states and still verify
        // the states compare as equals (core fields are ignored)
        initialState = new ComparableServiceState();
        initialState.documentOwner = UUID.randomUUID().toString();
        initialState.counter = 10L;

        modifiedState = new ComparableServiceState();
        modifiedState.documentOwner = UUID.randomUUID().toString();
        modifiedState.counter = 10L;

        value = ServiceDocument.equals(description, initialState, modifiedState);
        assertEquals(true, value);

        description = TestUtils.buildStateDescription(DatastoreServiceState.class, null);
        DatastoreServiceState initialDatastoreState = new DatastoreServiceState();
        initialDatastoreState.id = "vsan:043fc4b7673845a1-95f0756475e582e6";
        initialDatastoreState.name = "vsanDatastore";
        initialDatastoreState.type = "VSAN";
        initialDatastoreState.tags = new HashSet<>(Collections.singleton("VSAN"));
        initialDatastoreState.isImageDatastore = false;
        initialDatastoreState.documentKind = Utils.buildKind(DatastoreServiceState.class);
        initialDatastoreState.documentSelfLink =
                "/photon/cloudstore/datastores/vsan:043fc4b7673845a1-95f0756475e582e6";
        initialDatastoreState.documentUpdateTimeMicros = 1488729691543004L;
        initialDatastoreState.documentUpdateAction = "POST";
        initialDatastoreState.documentOwner = "855728ab-d836-4d3c-abb5-fd95d78354c2";

        DatastoreServiceState newDatastoreState = new DatastoreServiceState();
        newDatastoreState.id = initialDatastoreState.id;
        newDatastoreState.name = initialDatastoreState.name;
        newDatastoreState.type = initialDatastoreState.type;
        newDatastoreState.tags = initialDatastoreState.tags;
        newDatastoreState.isImageDatastore = true;
        newDatastoreState.documentKind = initialDatastoreState.documentKind;
        newDatastoreState.documentSelfLink = initialDatastoreState.documentSelfLink;
        newDatastoreState.documentOwner = initialDatastoreState.documentOwner;

        value = ServiceDocument.equals(description, initialDatastoreState, newDatastoreState);
        assertEquals(value, false);
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
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        stateB.documentUpdateTimeMicros = stateA.documentUpdateTimeMicros;

        EnumSet<DocumentRelationship> results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // different epochs, equal time, B should be preferred
        stateA.documentEpoch = 1L;
        stateB.documentEpoch = 2L;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        stateB.documentUpdateTimeMicros = stateA.documentUpdateTimeMicros;

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // same epochs, different versions, equal time, B should be preferred
        stateA.documentEpoch = 1L;
        stateB.documentEpoch = 1L;
        stateA.documentVersion = 1;
        stateB.documentVersion = 2;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        stateB.documentUpdateTimeMicros = stateA.documentUpdateTimeMicros;

        results = ServiceDocument.compare(stateA, stateB,
                description, Utils.getTimeComparisonEpsilonMicros());
        assertTrue(results.contains(DocumentRelationship.EQUAL_TIME));
        assertTrue(!results.contains(DocumentRelationship.NEWER_VERSION));
        assertTrue(!results.contains(DocumentRelationship.PREFERRED));
        assertTrue(!results.contains(DocumentRelationship.IN_CONFLICT));

        // equal versions, equal time, neither is preferred
        stateA.documentVersion = 1;
        stateB.documentVersion = 1;
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
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
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc()
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
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc()
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
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc()
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
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc()
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
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc()
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
        stateA.documentUpdateTimeMicros = Utils.getNowMicrosUtc()
                + Utils.getTimeComparisonEpsilonMicros() / 2;
        stateB.documentUpdateTimeMicros = Utils.getNowMicrosUtc();

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
