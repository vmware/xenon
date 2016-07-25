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

package com.vmware.xenon.test.sample;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ExampleServiceHost;
import com.vmware.xenon.services.common.ExampleServiceHost.ExampleHostArguments;
import com.vmware.xenon.test.ExceptionTestUtils;
import com.vmware.xenon.test.InProcessNodeGroup;
import com.vmware.xenon.test.TestClient;

@RunWith(JUnitPlatform.class)
public class ExampleServiceTests {

    private ServiceHost host;

    @BeforeEach
    public void setUp() {
        this.host = new ExampleServiceHost();
    }

    @AfterEach
    public void tearDown() {
        this.host.stop();
    }

    @Test
    public void exampleService() {

        // TODO: cleanup. Ideally xenon should not throw Throwable. wrapping it for now.
        try {
            File createdFolder = File.createTempFile("test-tmp", "");
            createdFolder.delete();
            createdFolder.mkdir();

            ExampleHostArguments args = new ExampleHostArguments();
            args.sandbox = createdFolder.toPath();

            this.host.initialize(args);
            this.host.setPort(8001);
            this.host.start();
        } catch (Throwable throwable) {
            throw ExceptionTestUtils.throwAsUnchecked(throwable);
        }


        // create nodegroup and wait conversion and service to be ready
        InProcessNodeGroup<ServiceHost> nodeGroup = new InProcessNodeGroup<>();
        nodeGroup.addHost(this.host);
        nodeGroup.joinHosts();  // TODO
        nodeGroup.waitForConversion();
        nodeGroup.waitForServiceAvailable("/core/examples");


        TestClient client = new TestClient(nodeGroup);

        // create example service
        ExampleServiceState postBody = new ExampleServiceState();
        postBody.name = "foo";
        postBody.documentSelfLink = "/foo";

        // synchronously perform operation and expect success
        client.sendPost("/core/examples", op -> op.setBody(postBody));
        nodeGroup.waitForServiceAvailable("/core/examples/foo");

        // GET and get body
        Operation get = Operation.createGet(nodeGroup.getHost(), "/core/examples/foo");
        // synchronously perform operation, then return body
        ExampleServiceState result = client.sendThenGetBody(get, ExampleServiceState.class);
        assertEquals("foo", result.name);


        // update using patch
        ExampleServiceState patchBody = new ExampleServiceState();
        patchBody.name = "bar";

        // synchronously send a patch with setting body to operation
        client.sendPatch("/core/examples/foo", op -> op.setBody(patchBody));

        // TODO: consider send...() to return OperationResponse
        get = Operation.createGet(nodeGroup.getHost(), "/core/examples/foo");
        result = client.sendThenGetBody(get, ExampleServiceState.class);
        assertEquals("bar", result.name);

        // verification in callback (this is synchronous call)
        client.sendExpectSuccess(get, op -> {
            ExampleServiceState body = op.getBody(ExampleServiceState.class);
            assertEquals("bar", body.name);
        });

        // delete the service
        client.sendDelete("/core/examples/foo");
        get = Operation.createGet(nodeGroup.getHost(), "/core/examples/foo");
        client.sendExpectFailure(get);  // TODO: check with 404
    }
}

