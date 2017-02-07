/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class ExampleTest {

    // command line argument
    public int iterationCount = 500;

    // command line argument
    public int maintenanceIntervalMillis = 20;

    private TestRequestSender sender;
    private VerificationHost host;

    public void setup() throws Throwable {
        this.host = VerificationHost.create(0);
        CommandLineArgumentParser.parseFromProperties(this.host);
        CommandLineArgumentParser.parseFromProperties(this);

        //workaround for https://www.pivotaltracker.com/n/projects/1471320/stories/138426713
        this.host.setPeerSynchronizationEnabled(false);
        this.host.setMaintenanceIntervalMicros(
                TimeUnit.MILLISECONDS.toMicros(this.maintenanceIntervalMillis));
        this.host.start();

        this.sender = this.host.getTestRequestSender();

        this.host.startServiceAndWait(ExampleFactoryService.class, ExampleFactoryService.SELF_LINK);
    }

    public void tearDown() {
        if (this.host == null) {
            return;
        }
        this.host.tearDown();
        this.host = null;
    }

    @Test
    public void loop() throws Throwable {
        for (int i = 0; i < this.iterationCount; i++) {
            tearDown();
            setup();
            this.host.log("^^^^^^^ iteration: %d", i);
            test();
        }
    }

    private void test() throws Throwable {
        ExampleState state = new ExampleState();
        state.name = "test";

        URI uri = UriUtils.buildUri(host, ExampleFactoryService.SELF_LINK);

        // create service
        Operation op = Operation.createPost(uri).setBody(state);
        ExampleState result = sender.sendAndWait(op, ExampleState.class);

        assertNotNull(result);
        assertNotNull(result.documentSelfLink);
        assertEquals(state.name, result.name);

        // try to create again, expect conflict error 409
        op = Operation.createPost(uri).setBody(state);
        TestRequestSender.FailureResponse failure = sender.sendAndWaitFailure(op);
        assertEquals(Operation.STATUS_CODE_CONFLICT, failure.op.getStatusCode());
    }

    /**
     * ExampleFactoryService class
     */
    public static class ExampleFactoryService extends FactoryService {
        public static final String SELF_LINK = ServiceUriPaths.CORE + "/test-examples";

        public ExampleFactoryService() {
            super(ExampleState.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new ExampleService();
        }

        @Override
        public void handlePost(Operation op) {
            if (!op.hasBody()) {
                throw new IllegalArgumentException("Body is required");
            }

            ExampleState body = op.getBody(ExampleState.class);
            body.documentSelfLink = UriUtils.buildUriPath(SELF_LINK, body.name);
            op.setBodyNoCloning(body);

            super.handlePost(op);
        }
    }

    /**
     * ExampleService
     */
    public static class ExampleService extends StatefulService {
        public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/test-examples";

        public ExampleService() {
            super(ExampleState.class);
            toggleOption(ServiceOption.PERSISTENCE, true);
            toggleOption(ServiceOption.REPLICATION, true);
            toggleOption(ServiceOption.OWNER_SELECTION, true);
            toggleOption(ServiceOption.IMMUTABLE, true);
            toggleOption(ServiceOption.ON_DEMAND_LOAD, true);
        }

        @Override
        public void handleStart(Operation post) {
            if (!post.hasBody()) {
                post.fail(new IllegalArgumentException("initial state is required"));
                return;
            }

            ExampleState s = post.getBody(ExampleState.class);
            if (s.name == null) {
                post.fail(new IllegalArgumentException("name is required"));
                return;
            }

            post.complete();
        }
    }

    /**
     * ExampleState
     */
    public static class ExampleState extends ServiceDocument {
        @Documentation(description = "Name")
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.REQUIRED)
        public String name;
    }

}
