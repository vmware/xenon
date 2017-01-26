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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;

public class TestXenonStrangeError {
    private TestServiceState state;
    private VerificationHost host;

    // command line argument
    public int iterationCount = 300;

    // command line argument
    public int maintIntervalMillis = 10;

    public void setup() throws Throwable {
        this.host = VerificationHost.create(0);
        CommandLineArgumentParser.parseFromProperties(this.host);
        CommandLineArgumentParser.parseFromProperties(this);
        this.host.setMaintenanceIntervalMicros(
                TimeUnit.MILLISECONDS.toMicros(this.maintIntervalMillis));
        this.host.start();
    }

    public void tearDown() {
        if (this.host == null) {
            return;
        }
        this.host.tearDown();
        this.host = null;
    }

    public void prepare() throws Throwable {
        setup();
        this.host.startServiceAndWait(TestFactoryService.class, TestFactoryService.SELF_LINK);
        this.state = new TestServiceState();
        this.state.documentSelfLink = "link";
        this.state.name = "value";
    }

    @Test
    public void postDeleteAfterStartLoop() throws Throwable {
        for (int i = 0; i < this.iterationCount; i++) {
            tearDown();
            prepare();
            this.host.log("iteration: %d", i);
            test(i);
        }
    }

    public void test(int version) throws Throwable {
        // post
        Operation response = doOperation(Action.POST, TestFactoryService.SELF_LINK,
                this.state);
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());
        assertTrue(response.hasBody());
        this.state = response.getBody(TestServiceState.class);

        // get
        response = doOperation(Action.GET, this.state.documentSelfLink, null);
        assertTrue(response.hasBody());
        this.state = response.getBody(TestServiceState.class);
        assertEquals("value", this.state.name);

        // delete
        response = doOperation(Action.DELETE, this.state.documentSelfLink, null);
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());
        assertTrue(response.hasBody());
        this.state = response.getBody(TestServiceState.class);
        assertEquals("value", this.state.name);

        // try get
        TestRequestSender.FailureResponse error = doOperationFailure(Action.GET, this.state.documentSelfLink);
        assertEquals(Operation.STATUS_CODE_NOT_FOUND, error.op.getStatusCode());
    }

    private Operation doOperation(Action action, String link, TestServiceState state)
            throws Throwable {

        return this.host.getTestRequestSender().sendAndWait(
                new Operation()
                        .setAction(action)
                        .setUri(UriUtils.buildUri(this.host, link))
                        .setBody(state)
        );
    }

    private TestRequestSender.FailureResponse doOperationFailure(Action action, String link)
            throws Throwable {

        return this.host.getTestRequestSender().sendAndWaitFailure(
                new Operation()
                        .setAction(action)
                        .setUri(UriUtils.buildUri(this.host, link))
        );
    }

    public static class TestFactoryService extends FactoryService {
        public static final String SELF_LINK = "test-factory";

        public TestFactoryService() {
            super(TestServiceState.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new TestStatefulService();
        }
    }

    private static class TestStatefulService extends StatefulService {
        public TestStatefulService() {
            super(TestServiceState.class);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
            super.toggleOption(ServiceOption.REPLICATION, true);
            super.toggleOption(ServiceOption.OWNER_SELECTION, true);
            super.toggleOption(ServiceOption.INSTRUMENTATION, true);
        }
    }

    public static class TestServiceState extends ServiceDocument {
        public String name;
    }

}
