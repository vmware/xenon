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
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestExampleService extends BasicReusableHostTestCase {

    public int serviceCount = 100;

    @Before
    public void prepare() throws Throwable {
        // make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        // completion
        this.host.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);
    }

    @Test
    public void factoryPost() throws Throwable {
        URI factoryUri = UriUtils.buildUri(this.host,
                ExampleFactoryService.class);

        this.host.testStart(this.serviceCount);
        String prefix = "example-";
        Long counterValue = Long.MAX_VALUE;
        URI[] childURIs = new URI[this.serviceCount];
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState initialState = new ExampleServiceState();
            initialState.name = initialState.documentSelfLink = prefix + i;
            initialState.counter = counterValue;
            final int finalI = i;
            // create an example service
            Operation createPost = Operation
                    .createPost(factoryUri)
                    .setBody(initialState).setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        ServiceDocument rsp = o.getBody(ServiceDocument.class);
                        childURIs[finalI] = UriUtils.buildUri(this.host, rsp.documentSelfLink);
                        this.host.completeIteration();
                    });
            this.host.send(createPost);
        }
        this.host.testWait();

        // do GET on all child URIs
        Map<URI, ExampleServiceState> childStates = this.host.getServiceState(null,
                ExampleServiceState.class, childURIs);
        for (ExampleServiceState s : childStates.values()) {
            assertEquals(counterValue, s.counter);
            assertTrue(s.name.startsWith(prefix));
            assertEquals(this.host.getId(), s.documentOwner);
            assertTrue(s.documentEpoch != null && s.documentEpoch == 0L);
        }

        // verify template GET works on factory
        ServiceDocumentQueryResult templateResult = this.host.getServiceState(null,
                ServiceDocumentQueryResult.class,
                UriUtils.extendUri(factoryUri, ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE));

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
    public void putExpectFailure() throws Throwable {
        // make sure example factory is started. the host does not wait for it
        // to start since its not a core service. Note that in production code
        // this is all asynchronous, you should not block and wait, just pass a
        // completion
        this.host.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);

        URI factoryUri = UriUtils.buildUri(this.host,
                ExampleFactoryService.class);
        this.host.testStart(1);
        URI[] childURI = new URI[1];
        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = UUID.randomUUID().toString();
        initialState.counter = Long.MAX_VALUE;

        // create an example service
        Operation createPost = Operation
                .createPost(factoryUri)
                .setBody(initialState).setCompletion((o, e) -> {
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
        host.testStart(1);
        Operation put = Operation.createPut(childURI[0])
                .setCompletion(host.getExpectedFailureCompletion()).setBody(emptyBody);
        host.send(put);
        host.testWait();
        host.toggleNegativeTestMode(false);
    }
}
