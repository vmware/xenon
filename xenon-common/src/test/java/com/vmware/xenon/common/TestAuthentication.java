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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static com.vmware.xenon.services.common.authn.BasicAuthenticationUtils.constructBasicAuth;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import org.junit.After;
import org.junit.Test;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.http.netty.NettyHttpListener;
import com.vmware.xenon.common.test.AuthTestUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.SystemUserService;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;
import com.vmware.xenon.services.common.authn.AuthenticationRequest;
import com.vmware.xenon.services.common.authn.AuthenticationRequest.AuthenticationRequestType;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils;

public class TestAuthentication {

    private static final String FOO_USER_ID = "foo@vmware.com";
    private static final String FOO_PASSWORD = "password";

    private static final String FOO_USER_PATH = "/" + FOO_USER_ID;

    private static final String SET_COOKIE_HEADER = "Set-Cookie";

    private List<VerificationHost> hostsToCleanup = new ArrayList<>();

    private VerificationHost createAndStartHost(boolean enableAuth, boolean secureAuthCookie,
            Service authenticationService) throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        host.setAuthorizationEnabled(enableAuth);

        // The NettyHttpListener for the host is not allocated until host start time, so this
        // attribute cannot be enabled without allocating a listener.
        if (secureAuthCookie) {
            NettyHttpListener listener = new NettyHttpListener(host);
            listener.setSecureAuthCookie(true);
            host.setListener(listener);
        }

        // set the authentication service
        if (authenticationService != null) {
            host.setAuthenticationService(authenticationService);
        }

        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                VerificationHost.FAST_MAINT_INTERVAL_MILLIS));

        host.start();

        // add to the list for cleanup after each test run
        this.hostsToCleanup.add(host);
        return host;
    }

    public static class TestAuthenticationService extends StatelessService {

        public static final String SELF_LINK = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHN,
                "test");

        public static String ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ4biIsInN1YiI6Ii9jb3JlL2F1dGh6L3Vz";

        @Override
        public void handleGet(Operation op) {
            // create an AuthorizationContext and set it using the ACCESS_TOKEN
            associateAuthorizationContext(this, op, ACCESS_TOKEN);
            op.complete();
        }

        @Override
        public void handlePost(Operation op) {
            // support token verification of through pragma
            if (!op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN)) {
                op.fail(new IllegalStateException("Invalid request"));
                return;
            }
            op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);
            String token = BasicAuthenticationUtils.getAuthToken(op);
            if (token == null) {
                op.fail(new IllegalArgumentException("Token is empty"));
                return;
            }

            if (token.equals(ACCESS_TOKEN)) {
                // create and return a claims object for system user since our test uses system user
                Claims claims = getClaims();
                op.setBody(claims);
                op.complete();
                return;
            }
            op.fail(new IllegalArgumentException("Invalid Token!"));
        }

        private void associateAuthorizationContext(Service service, Operation op, String token) {
            Claims claims = getClaims();

            AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
            ab.setClaims(claims);
            ab.setToken(token);
            ab.setPropagateToClient(true);

            // associate resulting authorization context with operation.
            service.setAuthorizationContext(op, ab.getResult());
        }

        private Claims getClaims() {
            Claims.Builder builder = new Claims.Builder();
            builder.setIssuer(AuthenticationConstants.DEFAULT_ISSUER);
            builder.setSubject(SystemUserService.SELF_LINK);
            return builder.getResult();
        }

        @Override
        public void authorizeRequest(Operation op) {
            op.complete();
        }

        @Override
        public boolean queueRequest(Operation op) {
            if (op.getUri().getPath().equals(SELF_LINK)) {
                return false;
            }
            op.addResponseHeader(Operation.LOCATION_HEADER, "http://www.vmware.com");
            op.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
            op.complete();
            return true;
        }
    }

    public static class FailQueueAuthenticationService extends TestAuthenticationService {
        @Override
        public boolean queueRequest(Operation op) {
            if (op.getUri().getPath().equals(SELF_LINK)) {
                return false;
            }
            op.setStatusCode(Operation.STATUS_CODE_FORBIDDEN);
            op.fail(new IllegalStateException("fail request"));
            return true;
        }
    }

    @Test
    public void testSettingAuthenticationService() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, new TestAuthenticationService());

        host.log("Testing setAuthenticationService");

        // Check if the authenticationService is set
        assertNotNull(host.getAuthenticationServiceUri());

        // Test if the right authenticationService is set
        assertEquals(TestAuthenticationService.SELF_LINK, host.getAuthenticationServiceUri()
                .getPath());

        host.log("Settting authenticationService is working");
    }

    @Test
    public void testNoAuthenticationService() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, null);

        host.log("Testing no authenticationService");

        // Check if the BasicAuthenticationService is set as authenticationService
        assertEquals(BasicAuthenticationService.SELF_LINK,
                host.getAuthenticationServiceUri().getPath());

        host.log("Expected behavior for no authenticationService");
    }

    @Test
    public void testAuthenticationServiceRedirect() throws Throwable {

        VerificationHost host = createAndStartHost(true, false, new TestAuthenticationService());
        host.log("Testing authenticationService redirect");

        TestRequestSender sender = new TestRequestSender(host);

        // make a un-authenticated request on the host
        Operation requestOp = Operation.createGet(host.getUri());
        Operation responseOp = sender.sendAndWait(requestOp);

        // check the redirect response
        assertEquals(Operation.STATUS_CODE_MOVED_TEMP, responseOp.getStatusCode());

        // check the location header to redirect
        assertEquals("http://www.vmware.com",
                responseOp.getResponseHeader(Operation.LOCATION_HEADER));

        host.log("AuthenticationService redirect is working.");
    }

    @Test
    public void testAuthServiceFailure() throws Throwable {

        VerificationHost host = createAndStartHost(true, false, new FailQueueAuthenticationService());

        TestRequestSender sender = new TestRequestSender(host);

        // make a un-authenticated request on the host and expect it to fail
        Operation requestOp = Operation.createGet(host.getUri());
        FailureResponse response = sender.sendAndWaitFailure(requestOp);
        assertEquals(Operation.STATUS_CODE_FORBIDDEN, response.op.getStatusCode());
    }

    @Test
    public void testAuthenticationServiceTokenRequest() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, new TestAuthenticationService());
        doAuthenticationServiceTokenRequest(host, false);
    }

    @Test
    public void testAuthenticationServiceTokenRequestSecureCookie() throws Throwable {
        VerificationHost host = createAndStartHost(true, true, new TestAuthenticationService());
        doAuthenticationServiceTokenRequest(host, true);
    }

    private void doAuthenticationServiceTokenRequest(VerificationHost host, boolean isSecure)
            throws Throwable {
        TestRequestSender sender = new TestRequestSender(host);
        host.log("Testing authenticationService token request");

        // make a request to get the accessToken for the authentication service
        Operation requestOp = Operation.createGet(host, TestAuthenticationService.SELF_LINK)
                .forceRemote();
        Operation responseOp = sender.sendAndWait(requestOp);

        String cookieHeader = responseOp.getResponseHeader(SET_COOKIE_HEADER);
        assertNotNull(cookieHeader);

        // assert the auth token cookie
        Cookie tokenCookie = ClientCookieDecoder.LAX.decode(cookieHeader);
        assertEquals(TestAuthenticationService.ACCESS_TOKEN, tokenCookie.value());
        assertEquals(isSecure, tokenCookie.isSecure());

        // assert the auth token header
        assertEquals(TestAuthenticationService.ACCESS_TOKEN,
                responseOp.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER));

        host.log("AuthenticationService token request is working");
    }

    private void createTestUsers(ServiceHost host) {
        // create user, user-group, resource-group, role for foo@vmware.com
        //   user: /core/authz/users/foo@vmware.com
        TestContext waitContext = new TestContext(1, Duration.ofSeconds(30));
        AuthorizationSetupHelper userBuilder = AuthorizationSetupHelper.create()
                .setHost(host)
                .setUserSelfLink(FOO_USER_ID)
                .setUserEmail(FOO_USER_ID)
                .setUserPassword(FOO_PASSWORD)
                .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
                .setCompletion(waitContext.getCompletion());

        // descriptively execute code under system auth context
        AuthTestUtils.setSystemAuthorizationContext(host);
        userBuilder.start();
        AuthTestUtils.resetAuthorizationContext(host);

        waitContext.await();
    }

    @Test
    public void testWithoutAuthorizationEnabled() throws Throwable {
        VerificationHost host = createAndStartHost(false, false, new TestAuthenticationService());
        host.log("Testing AuthenticationService when authorization is disabled");

        // create user foo@vmware.com
        createTestUsers(host);

        TestRequestSender sender = new TestRequestSender(host);

        // request for foo@vmware.com user document
        Operation requestOp = Operation.createGet(host, ServiceUriPaths.CORE_AUTHZ_USERS + FOO_USER_PATH);
        Operation responseOp = sender.sendAndWait(requestOp);

        // no redirect response
        assertEquals(Operation.STATUS_CODE_OK, responseOp.getStatusCode());

        // no location header too
        assertNull(responseOp.getResponseHeader(Operation.LOCATION_HEADER));
        host.log("Expected behavior when authorization is disabled");
    }

    @Test
    public void testAuthenticatedRequestInvalidToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, new TestAuthenticationService());
        host.log("Testing external authentication request with invalid token");

        // create user foo@vmware.com
        createTestUsers(host);

        // send invalid accesstoken
        TestRequestSender.setAuthToken("aasfsfsf");
        TestRequestSender sender = new TestRequestSender(host);

        // request for foo@vmware.com user document
        Operation requestOp = Operation.createGet(host, ServiceUriPaths.CORE_AUTHZ_USERS + FOO_USER_PATH);
        FailureResponse failureResponse = sender.sendAndWaitFailure(requestOp);

        // as per the TestAuthenticationService its a invalid token
        assertEquals(Operation.STATUS_CODE_FORBIDDEN, failureResponse.op.getStatusCode());

        TestRequestSender.clearAuthToken();
        host.log("Expected behavoir for external authentication request with invalid token");
    }

    @Test
    public void testAuthenticatedRequestValidToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, new TestAuthenticationService());
        host.log("Testing external authentication request with valid token");

        // create user foo@vmware.com
        createTestUsers(host);

        // send a valid accesstoken
        TestRequestSender.setAuthToken(TestAuthenticationService.ACCESS_TOKEN);
        TestRequestSender sender = new TestRequestSender(host);

        // request for foo@vmware.com user document
        Operation requestOp = Operation.createGet(host, ServiceUriPaths.CORE_AUTHZ_USERS + FOO_USER_PATH);
        Operation response = sender.sendAndWait(requestOp);

        // as per the TestAuthenticationService its a valid token
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());

        TestRequestSender.clearAuthToken();
        host.log("Expected behavoir for external authentication request with valid token");
    }

    @Test
    public void testAuthenticationViaBasicAuth() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, new TestAuthenticationService());

        // create user foo@vmware.com
        createTestUsers(host);

        TestRequestSender sender = new TestRequestSender(host);

        // login as foo@vmware.com
        String headerVal = constructBasicAuth(FOO_USER_ID, FOO_PASSWORD);
        AuthenticationRequest authReq = new AuthenticationRequest();
        authReq.requestType = AuthenticationRequestType.LOGIN;
        Operation requestOp = Operation
                .createPost(UriUtils.buildUri(host, BasicAuthenticationService.SELF_LINK))
                .setBody(authReq)
                .forceRemote()
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal);
        Operation response = sender.sendAndWait(requestOp);
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());
        assertNotNull(response.getResponseHeader(SET_COOKIE_HEADER));

        TestRequestSender.clearAuthToken();
    }

    @Test
    public void testVerificationValidBasicAuthAccessToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, null);
        host.log("Testing verification of valid token for Basic auth");

        // create a user so we are able to get valid accessToken to verify
        TestContext waitContext = new TestContext(1, Duration.ofSeconds(30));
        AuthorizationSetupHelper userBuilder = AuthorizationSetupHelper.create()
                .setHost(host)
                .setUserSelfLink("foo@vmware.com")
                .setUserEmail("foo@vmware.com")
                .setUserPassword("password")
                .setIsAdmin(false)
                .setDocumentLink(BasicAuthenticationService.SELF_LINK)
                .setCompletion(waitContext.getCompletion());

        // descriptively execute code under system auth context
        AuthTestUtils.setSystemAuthorizationContext(host);
        userBuilder.start();
        AuthTestUtils.resetAuthorizationContext(host);

        waitContext.await();

        String accessToken = AuthTestUtils.login(host, "foo@vmware.com", "password");
        TestRequestSender sender = new TestRequestSender(host);
        TestRequestSender.setAuthToken(accessToken);

        // make a request to verification service
        Operation requestOp = Operation.createPost(host, BasicAuthenticationService.SELF_LINK)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);

        Operation responseOp = sender.sendAndWait(requestOp);
        Claims claims = responseOp.getBody(Claims.class);
        assertNotNull(claims);

        TestRequestSender.clearAuthToken();

        host.log("Verification of valid token for Basic auth succeeded");
    }

    @Test
    public void testVerificationInvalidBasicAuthAccessToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, null);
        host.log("Testing verification of invalid token for Basic auth");

        // invalid accesstoken
        String invalidAccessToken = "aasfsfsf";
        TestRequestSender.setAuthToken(invalidAccessToken);
        TestRequestSender sender = new TestRequestSender(host);

        // make a request to verification service
        Operation requestOp = Operation.createPost(host, BasicAuthenticationService.SELF_LINK)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);

        FailureResponse failureResponse = sender.sendAndWaitFailure(requestOp);
        assertNotNull(failureResponse.failure);

        TestRequestSender.clearAuthToken();
        host.log("Verification of invalid token for Basic auth fails as expected");
    }

    @Test
    public void testVerificationValidAuthServiceToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, new TestAuthenticationService());
        host.log("Testing verification of valid token for external auth");

        TestRequestSender sender = new TestRequestSender(host);
        TestRequestSender.setAuthToken(TestAuthenticationService.ACCESS_TOKEN);

        // make a request to verification service
        Operation requestOp = Operation.createPost(host, TestAuthenticationService.SELF_LINK)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);

        Operation responseOp = sender.sendAndWait(requestOp);
        Claims claims = responseOp.getBody(Claims.class);
        assertNotNull(claims);

        TestRequestSender.clearAuthToken();
        host.log("Verification of valid token for external auth succeeded");
    }

    @Test
    public void testVerificationInvalidAuthServiceToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, false, new TestAuthenticationService());
        host.log("Testing verification of invalid token for external auth");

        // invalid accesstoken
        String invalidAccessToken = "aasfsfsf";
        TestRequestSender sender = new TestRequestSender(host);
        TestRequestSender.setAuthToken(invalidAccessToken);

        // make a request to verification service
        Operation requestOp = Operation.createPost(host, TestAuthenticationService.SELF_LINK)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);

        FailureResponse failureResponse = sender.sendAndWaitFailure(requestOp);
        assertNotNull(failureResponse.failure);

        TestRequestSender.clearAuthToken();
        host.log("Verification of invalid token for external auth fails as expected");
    }

    @Test
    public void testExternalAuthenticationMultinode() throws Throwable {
        VerificationHost host1 = createAndStartHost(true, false, new TestAuthenticationService());
        VerificationHost host2 = createAndStartHost(true, false, new TestAuthenticationService());
        VerificationHost host3 = createAndStartHost(true, false, new TestAuthenticationService());

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        nodeGroup.addHost(host1);
        nodeGroup.addHost(host2);
        nodeGroup.addHost(host3);

        // perform lambda under system auth context
        AuthTestUtils.executeWithSystemAuthContext(nodeGroup, () -> {
            nodeGroup.joinNodeGroupAndWaitForConvergence();
            // wait the service to be available in cluster
            nodeGroup.waitForFactoryServiceAvailable("/core/examples");
        });

        ServiceHost host = nodeGroup.getHost();

        // test external auth redirect
        host1.log("Testing auth service redirect in multi-node");
        testExternalAuthRedirectMultinode(host);
        host1.log("Auth service redirect in multi-node working as expected");

        // test external auth token request
        host1.log("Testing auth service token request in multi-node");
        testExternalAuthTokenRequestMultinode(host);
        host1.log("AuthenticationService token request is working in multi-node");

        // test replication with external auth
        host1.log("Testing replication with external auth in multi-node");
        testExternalAuthReplicationMultinode(host);
        host1.log("Replication with external auth in multi-node is working");
    }

    private void testExternalAuthRedirectMultinode(ServiceHost host) {
        TestRequestSender sender = new TestRequestSender(host);

        // make a un-authenticated request on the host
        Operation requestOp = Operation.createGet(host.getUri());
        Operation responseOp = sender.sendAndWait(requestOp);

        // check the redirect response
        assertEquals(Operation.STATUS_CODE_MOVED_TEMP, responseOp.getStatusCode());

        // check the location header to redirect
        assertEquals("http://www.vmware.com",
                responseOp.getResponseHeader(Operation.LOCATION_HEADER));
    }

    private void testExternalAuthTokenRequestMultinode(ServiceHost host) {
        TestRequestSender sender = new TestRequestSender(host);

        // make a request to get the accessToken for the authentication service
        Operation requestOp = Operation.createGet(host, TestAuthenticationService.SELF_LINK)
                .forceRemote();
        Operation responseOp = sender.sendAndWait(requestOp);

        String cookieHeader = responseOp.getResponseHeader(SET_COOKIE_HEADER);
        assertNotNull(cookieHeader);

        // assert the auth token cookie
        Cookie tokenCookie = ClientCookieDecoder.LAX.decode(cookieHeader);
        assertEquals(TestAuthenticationService.ACCESS_TOKEN, tokenCookie.value());

        // assert the auth token header
        assertEquals(TestAuthenticationService.ACCESS_TOKEN,
                responseOp.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER));
    }

    private void testExternalAuthReplicationMultinode(ServiceHost host) {
        // prepare operation sender(client)
        TestRequestSender.setAuthToken(TestAuthenticationService.ACCESS_TOKEN);
        TestRequestSender sender = new TestRequestSender(host);

        // POST request
        ExampleServiceState body = new ExampleServiceState();
        body.documentSelfLink = "/foo";
        body.name = "foo";
        Operation post = Operation.createPost(host, "/core/examples").setBody(body);

        // verify post response
        ExampleServiceState result = sender.sendAndWait(post, ExampleServiceState.class);
        assertEquals("foo", result.name);

        // make get and validate result
        Operation get = Operation.createGet(host, "/core/examples/foo");
        ExampleServiceState getResult = sender.sendAndWait(get, ExampleServiceState.class);

        // validate get result...
        assertEquals("foo", getResult.name);
        TestRequestSender.clearAuthToken();
    }

    @After
    public void tearDown() {
        this.hostsToCleanup.forEach(VerificationHost::tearDown);
        this.hostsToCleanup.clear();
    }
}
