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
import java.util.LinkedList;

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
        System.out.println("aaaaaaaaaaaaa"+ UriUtils.buildUri(this.host,factoryUri.getPath() +
                            "/SL"));
        LinkedList<URI> instanceURIs = new LinkedList<URI>();
        // try creating instance
        for (int i=0; i< 6000 ; i++) {
            this.host.testStart(1);
            EchoServiceState initialState = new EchoServiceState();
            initialState.documentSelfLink = "SL" + i;
            initialState.message = "Initial Message" + i;
            Operation createPost = Operation
                    .createPost(factoryUri)
                    .setBody(initialState).setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        ServiceDocument rsp = o.getBody(ServiceDocument.class);
                        instanceURIs.add( UriUtils.buildUri(this.host, rsp.documentSelfLink));
                        System.out.println("Created " + instanceURIs.getLast().toString());
                        this.host.completeIteration();
                    });
            this.host.send(createPost);
            this.host.testWait();
            EchoServiceState currentState = this.host.getServiceState(null,
                    EchoServiceState.class, instanceURIs.getLast());
            assertEquals(currentState.message, initialState.message);
        }
        
        for (int i=0; i< 6000 ; i++) {
            this.host.testStart(1);
            EchoServiceState initialState = new EchoServiceState();
            initialState.message = "Next Message" + i;
            Operation createPost = Operation
                    .createPatch(UriUtils.buildUri(this.host,factoryUri.getPath() +
                            "/SL" + i))
                    .setBody(initialState).setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        this.host.completeIteration();
                    });
            this.host.send(createPost);
            this.host.testWait();
        }
        
        for (int i=0; i< 6000 ; i++) {
            this.host.testStart(1);
            EchoServiceState initialState = new EchoServiceState();
            initialState.message = "Next Message" + i;
            Operation createDelete = Operation
                    .createDelete(UriUtils.buildUri(this.host,factoryUri.getPath() +
                            "/SL" + i))
                    .setBody(initialState).setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        this.host.completeIteration();
                    });
            this.host.send(createDelete);
            this.host.testWait();
        }
        
        this.host.testStart(1);
        Operation createGet = Operation
                .createGet(factoryUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        System.out.println("Iterration " + e.getMessage());
                        this.host.failIteration(e);
                    } else {
                        System.out.println("Iterration " + o.getBodyRaw());
                        this.host.completeIteration();
                    }
                });
        this.host.send(createGet);
        this.host.testWait();

        // Verify initial state
        // Make sure the default PUT worked
        EchoServiceState currentState = this.host.getServiceState(null,
                EchoServiceState.class, instanceURIs.getFirst());
    }
}
