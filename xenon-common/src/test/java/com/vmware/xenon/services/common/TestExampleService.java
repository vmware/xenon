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

import static java.util.stream.Collectors.toList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestExampleService extends BasicReusableHostTestCase {

    public int serviceCount = 100;
    private URI factoryUri;
    private final Long counterValue = Long.MAX_VALUE;
    private final String prefix = "example-";
    private TestRequestSender sender;

    @Before
    public void prepare() throws Throwable {

        this.factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);

        // make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        // completion

        // Starting example factory is already done in ExampleServiceHost for this test.
        // Here is an example how to start factory service:
        //    this.host.startFactory(ExampleService.class, ExampleService::createFactory);


        // Next, wait the factory service to be available in the node group (for this test, only
        // one node is in the node group).
        // For multiple nodes in the node group, make sure all nodes have joined the node group
        // and wait for the node group convergence using following methods:
        //   this.host.joinNodesAndVerifyConvergence(...);
        //   this.host.waitForNodeGroupConvergence(...);
        this.host.waitForReplicatedFactoryServiceAvailable(this.factoryUri);

        this.sender = new TestRequestSender(this.host);
    }

    @Test
    public void factoryPost() throws Throwable {
        List<ExampleServiceState> childStates = postExampleServicesThenGetStates("factory-post");

        // do GET on all child URIs
        for (ExampleServiceState s : childStates) {
            assertEquals(this.counterValue, s.counter);
            assertTrue(s.name.startsWith(this.prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertEquals(3, s.keyValues.size());
            assertEquals(Long.valueOf(0), s.documentEpoch);
        }

        // verify template GET works on factory
        URI uri = UriUtils.extendUri(this.factoryUri, ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE);
        ServiceDocumentQueryResult templateResult = this.sender.sendGetAndWait(uri.toString(),
                ServiceDocumentQueryResult.class);

        assertTrue(templateResult.documentLinks.size() == templateResult.documents.size());
        ExampleServiceState childTemplate = Utils.fromJson(
                templateResult.documents.get(templateResult.documentLinks.iterator().next()),
                ExampleServiceState.class);
        assertNotNull(childTemplate.keyValues);
        assertNotNull(childTemplate.counter);
        assertNotNull(childTemplate.name);
        assertNotNull(childTemplate.documentDescription);
        assertNotNull(childTemplate.documentDescription.propertyDescriptions);
        assertTrue(childTemplate.documentDescription.propertyDescriptions.size() > 0);
        assertTrue(childTemplate.documentDescription.propertyDescriptions.containsKey("name"));
        assertTrue(childTemplate.documentDescription.propertyDescriptions.containsKey("counter"));

        PropertyDescription pdMap = childTemplate.documentDescription.propertyDescriptions
                .get(ExampleServiceState.FIELD_NAME_KEY_VALUES);
        assertTrue(pdMap.usageOptions.contains(PropertyUsageOption.OPTIONAL));
        assertTrue(pdMap.indexingOptions.contains(PropertyIndexingOption.EXPAND));
    }

    @Test
    public void factoryPatchMap() throws Throwable {
        //create example services
        List<ExampleServiceState> childStates = postExampleServicesThenGetStates("patch-map");
        List<String> childPaths = childStates.stream().map(state -> state.documentSelfLink).collect(toList());


        //test that example services are created correctly
        for (ExampleServiceState s : childStates) {
            assertEquals(this.counterValue, s.counter);
            assertTrue(s.name.startsWith(this.prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertEquals(3, s.keyValues.size());
            assertEquals("test-value-1", s.keyValues.get("test-key-1"));
            assertEquals("test-value-2", s.keyValues.get("test-key-2"));
            assertEquals("test-value-3", s.keyValues.get("test-key-3"));
            assertEquals(Long.valueOf(0), s.documentEpoch);
        }

        //patch example services
        List<Operation> patches = new ArrayList<>();
        for (ExampleServiceState s : childStates) {
            s.keyValues.put("test-key-1", "test-value-1-patch-1");
            s.keyValues.put("test-key-2", "test-value-2-patch-1");
            s.keyValues.put("test-key-3", "test-value-3-patch-1");
            Operation createPatch = Operation
                    .createPatch(UriUtils.buildUri(this.host, s.documentSelfLink))
                    .setBody(s);
            patches.add(createPatch);
        }
        this.sender.sendAndWait(patches);

        //test that example services are patched correctly
        List<ExampleServiceState> patchedStates = getExampleServiceStates(childPaths);
        for (ExampleServiceState s : patchedStates) {
            assertEquals(this.counterValue, s.counter);
            assertTrue(s.name.startsWith(this.prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertEquals(3, s.keyValues.size());
            assertEquals("test-value-1-patch-1", s.keyValues.get("test-key-1"));
            assertEquals("test-value-2-patch-1", s.keyValues.get("test-key-2"));
            assertEquals("test-value-3-patch-1", s.keyValues.get("test-key-3"));
            assertEquals(Long.valueOf(0), s.documentEpoch);
        }

        //patch example services when deleting some values in the keyValues map
        List<Operation> patchesToSetNull = new ArrayList<>();
        for (ExampleServiceState s : patchedStates) {
            s.keyValues.put("test-key-1", "test-value-1-patch-1");
            s.keyValues.put("test-key-2", null);
            s.keyValues.put("test-key-3", null);
            Operation createPatch = Operation
                    .createPatch(UriUtils.buildUri(this.host, s.documentSelfLink))
                    .setBody(s);
            patchesToSetNull.add(createPatch);
        }
        this.sender.sendAndWait(patchesToSetNull);

        //test that deleted values in the keyValues map are gone
        List<ExampleServiceState> patchesToSetNullStates = getExampleServiceStates(childPaths);
        for (ExampleServiceState s : patchesToSetNullStates) {
            assertEquals(this.counterValue, s.counter);
            assertTrue(s.name.startsWith(this.prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertEquals(1, s.keyValues.size());
            assertEquals("test-value-1-patch-1", s.keyValues.get("test-key-1"));
            assertEquals(Long.valueOf(0), s.documentEpoch);
        }
    }

    @Test
    public void putExpectFailure() throws Throwable {
        // make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        // completion
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = Long.MAX_VALUE;

        // create an example service
        Operation createPost = Operation.createPost(this.factoryUri).setBody(initialState);
        ServiceDocument rsp = this.sender.sendAndWait(createPost, ServiceDocument.class);
        URI childURI = UriUtils.buildUri(this.host, rsp.documentSelfLink);


        host.toggleNegativeTestMode(true);
        // issue a PUT that we expect it to fail.
        ExampleServiceState emptyBody = new ExampleServiceState();
        Operation put = Operation.createPut(childURI).setBody(emptyBody);

        FailureResponse failureResponse = this.sender.sendAndWaitFailure(put);
        assertEquals("name must be set", failureResponse.failure.getMessage());

        host.toggleNegativeTestMode(false);
    }

    private List<ExampleServiceState> postExampleServicesThenGetStates(String suffix) {
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState initialState = new ExampleServiceState();
            initialState.name = initialState.documentSelfLink = this.prefix + i + suffix;
            initialState.counter = this.counterValue;
            initialState.keyValues.put("test-key-1", "test-value-1");
            initialState.keyValues.put("test-key-2", "test-value-2");
            initialState.keyValues.put("test-key-3", "test-value-3");
            // create an example service
            Operation createPost = Operation.createPost(this.factoryUri).setBody(initialState);
            ops.add(createPost);
        }
        return this.sender.sendAndWait(ops, ExampleServiceState.class);
    }

    private List<ExampleServiceState> getExampleServiceStates(List<String> servicePaths) {
        List<Operation> ops = servicePaths.stream()
                .map(path -> Operation.createGet(this.host, path))
                .collect(toList());
        return this.sender.sendAndWait(ops, ExampleServiceState.class);
    }
}
