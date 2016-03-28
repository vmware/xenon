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

package com.vmware.dcp.services.samples;

import static org.junit.Assert.assertEquals;

import java.net.URI;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.samples.SamplePreviousEchoService.EchoServiceState;
import com.vmware.xenon.services.samples.SampleSimpleEchoService;

public class TestSampleSimpleEchoService extends BasicReusableHostTestCase {

    @Before
    public void prepare() throws Throwable {
        this.host.startFactory(new SampleSimpleEchoService());
        this.host.waitForServiceAvailable(SampleSimpleEchoService.FACTORY_LINK);
    }

    @Test
    public void testState() throws Throwable {
        URI factoryUri = UriUtils.buildFactoryUri(this.host, SampleSimpleEchoService.class);
        this.host.testStart(1);
        URI[] instanceURIs = new URI[1];
        EchoServiceState initialState = new EchoServiceState();
        initialState.documentSelfLink = "one";
        initialState.message = "Initial Message";
        // try creating instance
        Operation createPost = Operation
                .createPost(factoryUri)
                .setBody(initialState).setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    ServiceDocument rsp = o.getBody(ServiceDocument.class);
                    instanceURIs[0] = UriUtils.buildUri(this.host, rsp.documentSelfLink);
                    this.host.completeIteration();
                });
        this.host.send(createPost);
        this.host.testWait();

        // Verify initial state
        // Make sure the default PUT worked
        EchoServiceState currentState = this.host.getServiceState(null,
                EchoServiceState.class, instanceURIs[0]);
        assertEquals(currentState.message, initialState.message);

        this.host.testStart(1);
        // Now send do a PUT
        EchoServiceState newState = new EchoServiceState();
        newState.message = "Message One";
        Operation createPut = Operation
                .createPut(instanceURIs[0])
                .setBody(newState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException(
                                    "Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });
        this.host.send(createPut);
        host.testWait();

        // This should be equal to the current state
        currentState = this.host.getServiceState(null, EchoServiceState.class, instanceURIs[0]);
        assertEquals(currentState.message, newState.message);
    }
}
