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

import org.junit.Test;

import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthCredentialsService;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.UserService;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils;

public class TestExternalAuth extends BasicTestCase {

    private String userServiceJane = null;
    private String userServiceJohn = null;
    private static final String USER_JANE = "jane";
    private static final String USER_JANE_EMAIL = "jane@doe.com";
    private static final String USER_JOHN = "john";
    private static final String USER_JOHN_EMAIL = "john@doe.com";


    @Override
    public void initializeHost(VerificationHost host) throws Exception {
        try {
            // create a xenon service host housing just the user authz rules
            VerificationHost externalAuthHost = createHost();
            externalAuthHost.setAuthorizationService(new AuthorizationContextService());
            externalAuthHost.setAuthorizationEnabled(true);
            ServiceHost.Arguments args = VerificationHost.buildDefaultServiceHostArguments(0);
            VerificationHost.initialize(externalAuthHost, args);
            externalAuthHost.start();
            externalAuthHost.setSystemAuthorizationContext();
            // create two users
            this.userServiceJane = createUsers(externalAuthHost, USER_JANE, USER_JANE_EMAIL);
            this.userServiceJohn = createUsers(externalAuthHost, USER_JOHN, USER_JOHN_EMAIL);
            externalAuthHost.resetAuthorizationContext();
            // start test verification host with an external auth provider
            args = VerificationHost.buildDefaultServiceHostArguments(0);
            args.authProviderHostUri = externalAuthHost.getUri().toString();
            args.isAuthorizationEnabled = true;
            VerificationHost.initialize(this.host, args);
            Utils.registerKind(UserService.UserState.class, Utils.buildKind(UserService.UserState.class));
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    private String createUsers(VerificationHost host, String userName, String email) throws Throwable {
        AuthorizationHelper authHelper = new AuthorizationHelper(host);
        String userServiceLink = authHelper.createUserService(host, email);
        authHelper.createRoles(host, email);
        AuthCredentialsServiceState authServiceState = new AuthCredentialsServiceState();
        authServiceState.userEmail = email;
        authServiceState.privateKey = email;
        URI authUri = UriUtils.buildUri(host, AuthCredentialsService.FACTORY_LINK);
        TestRequestSender sender = new TestRequestSender(host);
        sender.sendAndWait(Operation.createPost(authUri).setBody(authServiceState));
        return userServiceLink;
    }

    @Test
    public void testAuthentication() throws Throwable {
        String headerVal = BasicAuthenticationUtils.constructBasicAuth(USER_JANE_EMAIL, USER_JANE_EMAIL);
        URI authServiceUri = UriUtils.buildUri(this.host, BasicAuthenticationService.SELF_LINK);
        TestRequestSender sender = new TestRequestSender(host);
        Operation returnOp = sender.sendAndWait((Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)));
        assertTrue(returnOp.getStatusCode() == Operation.STATUS_CODE_OK);
    }

    @Test
    public void testDocumentAccess() throws Throwable {
        URI factoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        this.host.assumeIdentity(this.userServiceJane);
        this.host.doFactoryChildServiceStart(null,
                1,  ExampleServiceState.class, (op) -> {
                    ExampleServiceState st = new ExampleServiceState();
                    st.name = USER_JANE;
                    op.setBody(st);
                }, factoryUri);
        ServiceDocumentQueryResult res = this.host.getFactoryState(factoryUri);
        assertTrue(res.documentCount == 1);
        this.host.assumeIdentity(this.userServiceJohn);
        this.host.doFactoryChildServiceStart(null,
                1,  ExampleServiceState.class, (op) -> {
                    ExampleServiceState st = new ExampleServiceState();
                    st.name = USER_JOHN;
                    op.setBody(st);
                }, factoryUri);
        res = this.host.getExpandedFactoryState(factoryUri);
        assertTrue(res.documentCount == 1);
        this.host.setSystemAuthorizationContext();
        res = this.host.getFactoryState(factoryUri);
        assertTrue(res.documentCount == 2);
        this.host.resetAuthorizationContext();
    }
}
