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

import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationContextService;

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
        authHelper.createRoles(this.host);
        super.setUp();
        this.host.resetSystemAuthorizationContext();
        this.host.assumeIdentity(this.userServicePath, null);
    }

    @Test
    public void actions() throws Throwable {
        testGet();
        testPost();
        testPatch();
        testPut();
        testDelete();
    }

    @Test
    public void subscriptionLifecycle() throws Throwable {
        subscribeUnsubscribe("jane");
        subscribeStop("jane");
        subscribeClose("jane");
    }
}
