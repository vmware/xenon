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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.UUID;

import org.junit.Test;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthCredentialsService;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.AuthorizationTokenCacheService;
import com.vmware.xenon.services.common.AuthorizationTokenCacheService.AuthorizationTokenCacheServiceState;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserService;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils;

public class TestExternalAuth extends BasicTestCase {

    private String userServiceJane = null;
    private String userServiceJohn = null;
    VerificationHost externalAuthHost = null;
    private static final String USER_JANE = "jane";
    private static final String USER_JANE_EMAIL = "jane@doe.com";
    private static final String USER_JOHN = "john";
    private static final String USER_JOHN_EMAIL = "john@doe.com";


    @Override
    public void initializeHost(VerificationHost host) throws Exception {
        try {
            // create a xenon service host housing just the user authz rules
            this.externalAuthHost = createHost();
            ServiceHost.Arguments args = VerificationHost.buildDefaultServiceHostArguments(0);
            VerificationHost.initialize(this.externalAuthHost, args);
            this.externalAuthHost.setAuthorizationService(new AuthorizationContextService());
            this.externalAuthHost.setAuthorizationEnabled(true);
            this.externalAuthHost.start();
            this.externalAuthHost.setSystemAuthorizationContext();
            // create two users
            this.userServiceJane = createUsers(this.externalAuthHost, USER_JANE, USER_JANE_EMAIL);
            this.userServiceJohn = createUsers(this.externalAuthHost, USER_JOHN, USER_JOHN_EMAIL);
            this.externalAuthHost.resetAuthorizationContext();
            // start test verification host with an external auth provider
            args = VerificationHost.buildDefaultServiceHostArguments(0);
            args.authProviderHostUri = this.externalAuthHost.getUri().toString();
            args.isAuthorizationEnabled = true;
            VerificationHost.initialize(this.host, args);
            Utils.registerKind(UserService.UserState.class, Utils.buildKind(UserService.UserState.class));
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @Override
    public void beforeHostTearDown(VerificationHost host) {
        this.externalAuthHost.tearDown();
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
        TestRequestSender sender = new TestRequestSender(this.host);
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

        MinimalTestService s = new MinimalTestService();
        this.host.addPrivilegedService(MinimalTestService.class);
        this.host.startServiceAndWait(s, UUID.randomUUID().toString(), null);
        // check to make sure the auth context is not null on the the test host
        AuthorizationContext authContext = this.host.assumeIdentity(this.userServiceJane);
        assertNotNull(this.host.getAuthorizationContext(s, authContext.getToken()));
        this.host.setSystemAuthorizationContext();
        TestContext notifyWaitContext = testCreate(1);
        TestContext subscribeCreateContext = testCreate(1);
        TestRequestSender sender = new TestRequestSender(this.host);
        Operation subscribeOp  = Operation.createPost(this.externalAuthHost,
                AuthorizationTokenCacheService.SELF_LINK)
                    .setReferer(this.host.getUri())
                    .setCompletion((subscribe, subscribeEx) -> {
                        if (subscribeEx != null) {
                            subscribeCreateContext.failIteration(subscribeEx);
                            return;
                        }
                        subscribeCreateContext.completeIteration();
                    });
        // subscribe to any updates from the auth token service
        this.host.startSubscriptionService(subscribeOp, (op) -> {
            // act as a relay and patch the local AuthTokenCacheInstance
            TestContext broadcastContext = testCreate(1);
            this.host.broadcastRequest(ServiceUriPaths.DEFAULT_NODE_SELECTOR, false, Operation.createPatch(this.host,
                            AuthorizationTokenCacheService.SELF_LINK)
                            .setReferer(this.host.getUri())
                            .setBody(op.getBody(AuthorizationTokenCacheServiceState.class))
                            .setCompletion((notify, notifyEx) -> {
                                if (notifyEx != null) {
                                    broadcastContext.failIteration(notifyEx);
                                    return;
                                }
                                broadcastContext.completeIteration();
                            }));
            try {
                testWait(broadcastContext);
            } catch (Throwable e) {
                notifyWaitContext.failIteration(e);
            }
            // check to see if the local authz cache does not have a token for the user
            if (this.host.getAuthorizationContext(s, authContext.getToken()) != null) {
                notifyWaitContext.failIteration(new IllegalStateException("Auth context was not null"));
                return;
            }
            notifyWaitContext.completeIteration();
        });
        testWait(subscribeCreateContext);
        sender.sendAndWait(Operation.createDelete(this.externalAuthHost, this.userServiceJane));
        testWait(notifyWaitContext);
        this.host.resetAuthorizationContext();
    }
}
