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

package com.vmware.xenon.common;

import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationContextServiceHelper;
import com.vmware.xenon.services.common.AuthorizationContextServiceHelper.AuthServiceContext;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestCustomAuthContextService extends BasicTestCase {

    private static final String USER_JANE = "jane";
    private static final String USER_JANE_EMAIL = "jane@doe.com";
    private static final String USER_JOHN = "john";
    private static final String USER_JOHN_EMAIL = "john@doe.com";

    public static class CustomAuthorizationContextService extends StatelessService {

        public static final String SELF_LINK = "CustomAuthzService";

        public CustomAuthorizationContextService() {
            this.context = new AuthServiceContext(this, (op, userGroupLinks) -> {
                // allow access to only those groups owned by Jane
                if (userGroupLinks.iterator().next().contains(USER_JANE)) {
                    return userGroupLinks;
                }
                return null;
            }, null);
        }

        private final AuthServiceContext context;

        @Override
        public boolean queueRequest(Operation op) {
            return AuthorizationContextServiceHelper.queueRequest(op, this.context);
        }

        @Override
        public void handleRequest(Operation op) {
            if (op.getAction() == Action.DELETE && op.getUri().getPath().equals(getSelfLink())) {
                super.handleRequest(op);
                return;
            }
            AuthorizationContextServiceHelper.populateAuthContext(op, this.context);
        }
    }

    private String userServiceJane = null;
    private String userServiceJohn = null;

    @Override
    public void beforeHostStart(VerificationHost host) {
        host.setAuthorizationService(new CustomAuthorizationContextService());
        host.setAuthorizationEnabled(true);
        CommandLineArgumentParser.parseFromProperties(this);
    }

    @Before
    public void setupRoles() throws Throwable {
        this.host.setSystemAuthorizationContext();
        AuthorizationHelper authHelper = new AuthorizationHelper(this.host);
        this.userServiceJane = authHelper.createUserService(this.host, USER_JANE_EMAIL);
        authHelper.createRoles(this.host, USER_JANE_EMAIL);
        this.userServiceJohn = authHelper.createUserService(this.host, USER_JOHN_EMAIL);
        authHelper.createRoles(this.host, USER_JOHN_EMAIL);
        this.host.resetAuthorizationContext();
    }

    @Test
    public void testAccess() throws Exception {
        URI factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        this.host.assumeIdentity(this.userServiceJane);
        ExampleServiceState st = new ExampleServiceState();
        st.name = USER_JANE;
        this.host.sendAndWaitExpectSuccess(Operation.createPost(factoryUri)
                .setBody(st));
        ServiceDocumentQueryResult res = this.host.getFactoryState(factoryUri);
        assertTrue(res.documentCount == 1);
        // access as John should fail
        this.host.assumeIdentity(this.userServiceJohn);
        st = new ExampleServiceState();
        st.name = USER_JOHN;
        this.host.sendAndWaitExpectFailure(Operation.createPost(factoryUri)
                .setBody(st));
    }
}
