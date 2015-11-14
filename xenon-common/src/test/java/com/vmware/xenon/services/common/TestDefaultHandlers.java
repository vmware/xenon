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

import java.net.URI;

import org.junit.Test;

import com.vmware.xenon.common.BasicReportTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * This is a set of tests for the default handlers -- as new default handlers are being added,
 * new test cases will be added here.
 */
public class TestDefaultHandlers extends BasicReportTestCase {

    /**
     * The state includes both a reference and a primitive type so as to test both.
     */
    public static class DefaultHandlerState extends ServiceDocument {
        int stateInt;
        String stateString;
    }

    /**
     * This is basically _the minimum_ test service (but "minimum" is already taken) since it uses
     * and tests the _default_ handlers (e.g., PUT, GET).
     */
    public class DefaultHandlerTestService extends StatefulService {

        public DefaultHandlerTestService() {
            super(DefaultHandlerState.class);
        }

        @Override
        public void handleStart(Operation startPost) {
            if (startPost.hasBody()) {
                DefaultHandlerState s = startPost.getBody(DefaultHandlerState.class);
                logFine("Initial state is %s", Utils.toJsonHtml(s));
            }
            startPost.complete();
        }
    }

    /**
     * Test the default PUT handler
     */
    @Test
    public void testPUT() throws Throwable {
        URI uri = UriUtils.buildUri(this.host, "testHandlersInstance");
        this.host.startService(Operation.createPost(uri), new DefaultHandlerTestService());
        String uriPath = uri.getPath();
        this.host.waitForServiceAvailable(uriPath);

        this.host.testStart(1);
        // Now send do a PUT
        DefaultHandlerState newState = new DefaultHandlerState();
        newState.stateString = "State One";
        newState.stateInt = 1;
        Operation createPut = Operation
                .createPut(uri)
                .setBody(newState)
                .setCompletion(
                        (o, e) -> {
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

        // Make sure the default PUT worked
        DefaultHandlerState currentState = this.host.getServiceState(null,
                DefaultHandlerState.class, uri);
        assertEquals(currentState.stateInt, newState.stateInt);
        assertEquals(currentState.stateString, newState.stateString);
    }
}
