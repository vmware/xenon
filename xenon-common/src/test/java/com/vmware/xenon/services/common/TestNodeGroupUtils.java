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

package com.vmware.xenon.services.common;

import static java.util.stream.Collectors.toList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupBroadcastResult.PeerNodeResult;

public class TestNodeGroupUtils {

    @Test
    public void toBroadcastResult() {
        String hostSuccess1 = "host-success1";
        String hostSuccess2 = "host-success2";
        String hostSuccess3 = "host-success3";
        String hostFailure = "host-fail";
        URI successUri1 = URI.create("http://success1.localhost/core/examples");
        URI successUri2 = URI.create("http://success2.localhost/core/examples");
        URI successUri3 = URI.create("http://success3.localhost/core/examples");
        URI failureUri = URI.create("http://fail.localhost/core/examples");
        URI successNodeGroup1 = URI.create("http://success1.localhost/core/node-groups/default");
        URI successNodeGroup2 = URI.create("http://success2.localhost/core/node-groups/default");
        URI successNodeGroup3 = URI.create("http://success3.localhost/core/node-groups/default");
        URI failureNodeGroup = URI.create("http://fail.localhost/core/node-groups/default");

        ServiceDocument document = new ServiceDocument();
        document.documentOwner = hostSuccess1;
        String jsonForHostSuccess1 = Utils.toJson(document);
        document.documentOwner = hostSuccess2;
        String jsonForHostSuccess2 = Utils.toJson(document);
        document.documentOwner = hostSuccess3;
        String jsonForHostSuccess3 = Utils.toJson(document);

        ServiceErrorResponse errorResponse = new ServiceErrorResponse();


        NodeGroupBroadcastResponse seed = new NodeGroupBroadcastResponse();
        seed.nodeCount = 5;
        seed.availableNodeCount = 4;
        seed.membershipQuorum = 2;

        seed.receivers.add(successUri1);
        seed.receivers.add(successUri2);
        seed.receivers.add(successUri3);
        seed.receivers.add(failureUri);

        seed.selectedNodes.put(hostSuccess1, successNodeGroup1);
        seed.selectedNodes.put(hostSuccess2, successNodeGroup2);
        seed.selectedNodes.put(hostSuccess3, successNodeGroup3);
        seed.selectedNodes.put(hostFailure, failureNodeGroup);

        seed.jsonResponses.put(successUri1, jsonForHostSuccess1);
        seed.jsonResponses.put(successUri2, jsonForHostSuccess2);
        seed.jsonResponses.put(successUri3, jsonForHostSuccess3);

        seed.failures.put(failureUri, errorResponse);

        NodeGroupBroadcastResult result = NodeGroupUtils.toBroadcastResult(seed);

        assertEquals(5, result.totalNodeCount);
        assertEquals(4, result.availableNodeCount);
        assertEquals(1, result.unavailableNodeCount);
        assertEquals(2, result.membershipQuorum);

        assertTrue(result.hasSuccess());
        assertTrue(result.hasFailure());
        assertTrue(result.isMajoritySuccess());
        assertFalse(result.isAllSuccess());
        assertFalse(result.isAllFailure());
        assertFalse(result.isMajorityFailure());

        Set<PeerNodeResult> successes = result.successResponses;
        assertEquals(3, successes.size());
        List<PeerNodeResult> sorted = successes.stream()
                .sorted((left, right) -> left.hostId.compareTo(right.hostId))
                .collect(toList());

        // success1
        PeerNodeResult singleResponse = sorted.get(0);
        assertTrue(singleResponse.isSuccess());
        assertFalse(singleResponse.isFailure());
        assertEquals(hostSuccess1, singleResponse.hostId);
        assertEquals(successUri1, singleResponse.requestUri);
        assertEquals(successNodeGroup1, singleResponse.nodeGroupUri);
        assertEquals(jsonForHostSuccess1, singleResponse.json);
        assertNull(singleResponse.errorResponse);

        // success2
        singleResponse = sorted.get(1);
        assertTrue(singleResponse.isSuccess());
        assertFalse(singleResponse.isFailure());
        assertEquals(hostSuccess2, singleResponse.hostId);
        assertEquals(successUri2, singleResponse.requestUri);
        assertEquals(successNodeGroup2, singleResponse.nodeGroupUri);
        assertEquals(jsonForHostSuccess2, singleResponse.json);
        assertNull(singleResponse.errorResponse);

        // success3
        singleResponse = sorted.get(2);
        assertTrue(singleResponse.isSuccess());
        assertFalse(singleResponse.isFailure());
        assertEquals(hostSuccess3, singleResponse.hostId);
        assertEquals(successUri3, singleResponse.requestUri);
        assertEquals(successNodeGroup3, singleResponse.nodeGroupUri);
        assertEquals(jsonForHostSuccess3, singleResponse.json);
        assertNull(singleResponse.errorResponse);

        // failure
        Set<PeerNodeResult> failures = result.failureResponses;
        assertEquals(1, failures.size());
        PeerNodeResult failure = failures.iterator().next();
        assertTrue(failure.isFailure());
        assertEquals(hostFailure, failure.hostId);
        assertEquals(failureUri, failure.requestUri);
        assertEquals(failureNodeGroup, failure.nodeGroupUri);
        assertNull(failure.json);
        assertSame(errorResponse, failure.errorResponse);
    }
}
