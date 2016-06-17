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

package com.vmware.xenon.common.test.websockets;

import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

/**
 * Tests websocket service with authentication on.
 */
public class TestAuthWebSocketService extends AbstractWebSocketServiceTest {
    private String userServicePath;

    @Override
    public void beforeHostStart(VerificationHost host) {
        // Enable authorization service; this is an end to end test
        host.setAuthorizationService(new AuthorizationContextService());
        host.setAuthorizationEnabled(true);
    }

    @Override
    public void setUp() throws Throwable {
        this.host.setSystemAuthorizationContext();
        AuthorizationHelper authHelper = new AuthorizationHelper(this.host);
        this.userServicePath = authHelper.createUserService(this.host, "jane@doe.com");
        authHelper.createRoles(this.host, "jane@doe.com");
        super.setUp();
        this.host.resetSystemAuthorizationContext();
        this.host.assumeIdentity(this.userServicePath);
    }

    @Test
    public void actions() throws Throwable {
        testGet();
        testPost();
        testPatch();
        testPut();
        testDelete();

        // this method must be run after web socket traffic has occurred on the HTTP client
        testAuthContextPropagation();
    }

    private void testAuthContextPropagation() throws Throwable {
        this.host.getClient().setConnectionLimitPerHost(2);
        // do a regular HTTP operation, with no context, it should fail. we are validating
        // that the web socket client handler does not leak its context on all operations
        this.host.resetSystemAuthorizationContext();
        this.host.testStart(1);
        Operation post = Operation.createPost(
                UriUtils.buildFactoryUri(this.host, ExampleService.class));
        ExampleServiceState st = new ExampleServiceState();
        st.documentKind = Utils.buildKind(ExampleServiceState.class);
        st.name = "jane";
        post.setBody(st);
        // we expect this to fail: we reset the authorization context, and the example service
        // instance is only authorization for jane. If the HTTP request handlers were leaking
        // context, the jane context would have "polluted" all in bound requests, making this work!
        post.setCompletion(this.host.getExpectedFailureCompletion()).forceRemote();
        this.host.send(post);
        this.host.testWait();
    }

    @Test
    public void subscriptionLifecycle() throws Throwable {
        subscribeUnsubscribe("jane");
        subscribeStop("jane");
        subscribeClose("jane");
    }
}
