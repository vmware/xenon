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

import java.net.URI;
import java.util.Map;
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
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestExampleService extends BasicReusableHostTestCase {

    public int serviceCount = 100;
    URI factoryUri;
    final Long counterValue = Long.MAX_VALUE;
    final String prefix = "example-";
    URI[] childURIs;

    @Before
    public void prepare() throws Throwable {
        // make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        // completion
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        this.factoryUri = UriUtils.buildFactoryUri(this.host,
                ExampleService.class);
    }

    @Test
    public void factoryPost() throws Throwable {
        final TestContext testCtx = host.testCreate(this.serviceCount);
        postExampleServices("factory-post", testCtx);
        testCtx.await();
        // do GET on all child URIs
        Map<URI, ExampleServiceState> childStates = this.host.getServiceState(null,
                ExampleServiceState.class, this.childURIs);
        for (ExampleServiceState s : childStates.values()) {
            assertEquals(this.counterValue, s.counter);
            assertTrue(s.name.startsWith(this.prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertEquals(s.keyValues.size(), 3);
            assertTrue(s.documentEpoch != null && s.documentEpoch == 0L);
        }

        // verify template GET works on factory
        ServiceDocumentQueryResult templateResult = this.host.getServiceState(null,
                ServiceDocumentQueryResult.class,
                UriUtils.extendUri(this.factoryUri, ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE));

        assertTrue(templateResult.documentLinks.size() == templateResult.documents.size());
        ExampleServiceState childTemplate = Utils.fromJson(
                templateResult.documents.get(templateResult.documentLinks.iterator().next()),
                ExampleServiceState.class);
        assertTrue(childTemplate.keyValues != null);
        assertTrue(childTemplate.counter != null);
        assertTrue(childTemplate.name != null);
        assertTrue(childTemplate.documentDescription != null);
        assertTrue(childTemplate.documentDescription.propertyDescriptions != null
                && childTemplate.documentDescription.propertyDescriptions
                        .size() > 0);
        assertTrue(childTemplate.documentDescription.propertyDescriptions
                .containsKey("name"));
        assertTrue(childTemplate.documentDescription.propertyDescriptions
                .containsKey("counter"));

        PropertyDescription pdMap = childTemplate.documentDescription.propertyDescriptions
                .get(ExampleServiceState.FIELD_NAME_KEY_VALUES);
        assertTrue(pdMap.usageOptions.contains(PropertyUsageOption.OPTIONAL));
        assertTrue(pdMap.indexingOptions.contains(PropertyIndexingOption.EXPAND));
    }

    @Test
    public void factoryPatchMap() throws Throwable {
        final TestContext testCtx1 = host.testCreate(this.serviceCount);
        //create example services
        postExampleServices("patch-map", testCtx1);
        testCtx1.await();
        //test that example services are created correctly
        Map<URI, ExampleServiceState> childStates = this.host.getServiceState(null,
                ExampleServiceState.class, this.childURIs);
        for (ExampleServiceState s : childStates.values()) {
            assertEquals(this.counterValue, s.counter);
            assertTrue(s.name.startsWith(this.prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertEquals(s.keyValues.size(), 3);
            assertEquals(s.keyValues.get("test-key-1"), "test-value-1");
            assertEquals(s.keyValues.get("test-key-2"), "test-value-2");
            assertEquals(s.keyValues.get("test-key-3"), "test-value-3");
            assertTrue(s.documentEpoch != null && s.documentEpoch == 0L);
        }

        //patch example services
        final TestContext testCtx2 = host.testCreate(this.serviceCount);
        for (ExampleServiceState s : childStates.values()) {
            s.keyValues.put("test-key-1", "test-value-1-patch-1");
            s.keyValues.put("test-key-2", "test-value-2-patch-1");
            s.keyValues.put("test-key-3", "test-value-3-patch-1");
            Operation createPatch = Operation
                    .createPatch(UriUtils.buildUri(this.host,
                            s.documentSelfLink))
                    .setBody(s)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        testCtx2.completeIteration();
                    });
            this.host.send(createPatch);
        }

        testCtx2.await();
        //test that example services are patched correctly
        childStates = this.host.getServiceState(null,
                ExampleServiceState.class, this.childURIs);
        for (ExampleServiceState s : childStates.values()) {
            assertEquals(this.counterValue, s.counter);
            assertTrue(s.name.startsWith(this.prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertEquals(s.keyValues.size(), 3);
            assertEquals(s.keyValues.get("test-key-1"), "test-value-1-patch-1");
            assertEquals(s.keyValues.get("test-key-2"), "test-value-2-patch-1");
            assertEquals(s.keyValues.get("test-key-3"), "test-value-3-patch-1");
            assertTrue(s.documentEpoch != null && s.documentEpoch == 0L);
        }

        //patch example services when deleting some values in the keyValues map
        final TestContext testCtx3 = host.testCreate(this.serviceCount);
        for (ExampleServiceState s : childStates.values()) {
            s.keyValues.put("test-key-1", "test-value-1-patch-1");
            s.keyValues.put("test-key-2", null);
            s.keyValues.put("test-key-3", null);
            Operation createPatch = Operation
                    .createPatch(UriUtils.buildUri(this.host,
                            s.documentSelfLink))
                    .setBody(s)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        testCtx3.completeIteration();
                    });
            this.host.send(createPatch);
        }

        testCtx3.await();
        //test that deleted values in the keyValues map are gone
        childStates = this.host.getServiceState(null,
                ExampleServiceState.class, this.childURIs);
        for (ExampleServiceState s : childStates.values()) {
            assertEquals(this.counterValue, s.counter);
            assertTrue(s.name.startsWith(this.prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertEquals(s.keyValues.size(), 1);
            assertEquals(s.keyValues.get("test-key-1"), "test-value-1-patch-1");
            assertTrue(s.documentEpoch != null && s.documentEpoch == 0L);
        }
    }

    @Test
    public void putExpectFailure() throws Throwable {
        // make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        // completion
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        this.host.testStart(1);
        URI[] childURI = new URI[1];
        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = Long.MAX_VALUE;

        // create an example service
        Operation createPost = Operation
                .createPost(this.factoryUri)
                .setBody(initialState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    ServiceDocument rsp = o.getBody(ServiceDocument.class);
                    childURI[0] = UriUtils.buildUri(this.host, rsp.documentSelfLink);
                    this.host.completeIteration();
                });
        this.host.send(createPost);

        this.host.testWait();

        host.toggleNegativeTestMode(true);
        // issue a PUT that we expect it to fail.
        ExampleServiceState emptyBody = new ExampleServiceState();
        this.host.testStart(1);
        Operation put = Operation.createPut(childURI[0])
                .setCompletion(host.getExpectedFailureCompletion())
                .setBody(emptyBody);
        host.send(put);
        host.testWait();
        host.toggleNegativeTestMode(false);
    }

    private void postExampleServices(String suffix, TestContext testCtx) throws Throwable {
        this.childURIs = new URI[this.serviceCount];
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState initialState = new ExampleServiceState();
            initialState.name = initialState.documentSelfLink = this.prefix + i + suffix;
            initialState.counter = this.counterValue;
            initialState.keyValues.put("test-key-1", "test-value-1");
            initialState.keyValues.put("test-key-2", "test-value-2");
            initialState.keyValues.put("test-key-3", "test-value-3");
            final int finalI = i;
            // create an example service
            Operation createPost = Operation
                    .createPost(this.factoryUri)
                    .setBody(initialState)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        ServiceDocument rsp = o.getBody(ServiceDocument.class);
                        this.childURIs[finalI] = UriUtils.buildUri(this.host, rsp.documentSelfLink);
                        testCtx.completeIteration();
                    });
            this.host.send(createPost);
        }
    }
}
