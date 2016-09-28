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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.vmware.xenon.common.test.AuthTestUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils;

public class TestAuthentication {

    private List<VerificationHost> hostsToCleanup = new ArrayList<>();

    private VerificationHost createAndStartHost(boolean enableAuth, Service authenticationService)
            throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        host.setAuthorizationEnabled(enableAuth);

        // set the authentication service
        host.setAuthenticationService(authenticationService);

        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
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
            // set the accessToken in header and cookie as the framework expects it.
            op.addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, ACCESS_TOKEN);
            op.setCookies(Collections.singletonMap(
                    AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE, ACCESS_TOKEN));
            op.complete();
        }

        @Override
        public void handlePost(Operation op) {
            // support token verification of through pragma
            if (!op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN)) {
                op.fail(new IllegalStateException("Invalid request"));
                return;
            }
            String token = BasicAuthenticationUtils.getAuthToken(op);
            if (token == null) {
                op.fail(new IllegalArgumentException("Token is empty"));
                return;
            }

            if (token.equals(ACCESS_TOKEN)) {
                // return a dummy claims object
                Claims.Builder claimsBuilder = new Claims.Builder();
                claimsBuilder.setSubject("foo@vmware.com");
                Claims claims = claimsBuilder.getResult();
                op.setBody(claims);
                op.complete();
                return;
            }
            op.fail(new IllegalArgumentException("Invalid Token!"));
        }

        @Override
        public void authorizeRequest(Operation op) {
            op.complete();
        }

        @Override
        public boolean queueRequest(Operation op) {
            if (!op.getUri().getPath().equals(SELF_LINK)) {
                op.addResponseHeader(Operation.LOCATION_HEADER, "http://www.vmware.com");
                op.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
                op.complete();
                return true;
            }
            return false;
        }
    }

    @Test
    public void testSettingAuthenticationService() throws Throwable {
        VerificationHost host = createAndStartHost(true, new TestAuthenticationService());

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
        VerificationHost host = createAndStartHost(true, null);

        host.log("Testing no authenticationService");

        // Check if the authenticationService is set
        assertNull(host.getAuthenticationServiceUri());

        host.log("Expected behavior for no authenticationService");
    }

    @Test
    public void testAuthenticationServiceRedirect() throws Throwable {

        VerificationHost host = createAndStartHost(true, new TestAuthenticationService());
        host.log("Testing authenticationService redirect");

        TestRequestSender sender = new TestRequestSender(host);

        // make a un-authenticated request on the host
        Operation requestOp = Operation.createGet(UriUtils.buildUri(host, "/"));
        Operation responseOp = sender.sendAndWait(requestOp);

        // check the redirect response
        assertEquals(Operation.STATUS_CODE_MOVED_TEMP, responseOp.getStatusCode());

        // check the location header to redirect
        assertEquals("http://www.vmware.com",
                responseOp.getResponseHeader(Operation.LOCATION_HEADER));

        host.log("AuthenticationService redirect is working.");
    }

    @Test
    public void testAuthenticationServiceTokenRequest() throws Throwable {
        VerificationHost host = createAndStartHost(true, new TestAuthenticationService());
        TestRequestSender sender = new TestRequestSender(host);
        host.log("Testing authenticationService token request");

        // make a request to get the accessToken for the authentication service
        Operation requestOp = Operation.createGet(UriUtils.buildUri(host,
                TestAuthenticationService.SELF_LINK));
        Operation responseOp = sender.sendAndWait(requestOp);

        // no redirect response for the authentication service request
        assertEquals(Operation.STATUS_CODE_OK, responseOp.getStatusCode());

        // no location header too
        assertNull(responseOp.getResponseHeader(Operation.LOCATION_HEADER));

        // the authn header cookie should be populated
        assertEquals(TestAuthenticationService.ACCESS_TOKEN,
                responseOp.getRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER));

        // the authn cookie should be populated
        assertEquals(TestAuthenticationService.ACCESS_TOKEN,
                responseOp.getCookies().get(AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE));

        host.log("AuthenticationService token request is working");
    }

    @Test
    public void testWithoutAuthorizationEnabled() throws Throwable {
        VerificationHost host = createAndStartHost(false, new TestAuthenticationService());
        host.log("Testing AuthenticationService when authorization is disabled");
        TestRequestSender sender = new TestRequestSender(host);

        // make a request to get the accessToken for the authentication service
        Operation requestOp = Operation.createGet(UriUtils.buildUri(host, "/"));
        Operation responseOp = sender.sendAndWait(requestOp);

        // no redirect response
        assertEquals(Operation.STATUS_CODE_OK, responseOp.getStatusCode());

        // no location header too
        assertNull(responseOp.getResponseHeader(Operation.LOCATION_HEADER));
        host.log("Expected behavior when authorization is disabled");
    }

    @Test
    public void testAuthenticatedRequestInvalidToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, new TestAuthenticationService());
        host.log("Testing external authentication request with invalid token");

        // send invalid accesstoken
        TestRequestSender.setAuthToken("aasfsfsf");
        TestRequestSender sender = new TestRequestSender(host);

        // make a request to get the accessToken for the authentication service
        Operation requestOp = Operation.createGet(UriUtils.buildUri(host, "/"));
        FailureResponse failureResponse = sender.sendAndWaitFailure(requestOp);

        // as per the TestAuthenticationService its a invalid token
        assertEquals(Operation.STATUS_CODE_FORBIDDEN, failureResponse.op.getStatusCode());

        TestRequestSender.clearAuthToken();
        host.log("Expected behavoir for external authentication request with invalid token");
    }

    @Test
    public void testAuthenticatedRequestValidToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, new TestAuthenticationService());
        host.log("Testing external authentication request with valid token");

        // send a valid accesstoken
        TestRequestSender.setAuthToken(TestAuthenticationService.ACCESS_TOKEN);
        TestRequestSender sender = new TestRequestSender(host);

        // make a request to get the accessToken for the authentication service
        Operation requestOp = Operation.createGet(UriUtils.buildUri(host, "/"));
        FailureResponse failureResponse = sender.sendAndWaitFailure(requestOp);

        // as per the TestAuthenticationService its a valid token
        assertEquals(Operation.STATUS_CODE_NOT_FOUND, failureResponse.op.getStatusCode());

        TestRequestSender.clearAuthToken();
        host.log("Expected behavoir for external authentication request with valid token");
    }

    @Test
    public void testVerificationValidBasicAuthAccessToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, null);
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
        Operation requestOp = Operation.createPost(
                UriUtils.buildUri(host, BasicAuthenticationService.SELF_LINK))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);

        Operation responseOp = sender.sendAndWait(requestOp);
        Claims claims = responseOp.getBody(Claims.class);
        assertNotNull(claims);

        TestRequestSender.clearAuthToken();

        host.log("Verification of valid token for Basic auth succeeded");
    }

    @Test
    public void testVerificationInvalidBasicAuthAccessToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, null);
        host.log("Testing verification of invalid token for Basic auth");

        // invalid accesstoken
        String invalidAccessToken = "aasfsfsf";
        TestRequestSender.setAuthToken(invalidAccessToken);
        TestRequestSender sender = new TestRequestSender(host);

        // make a request to verification service
        Operation requestOp = Operation.createPost(
                UriUtils.buildUri(host, BasicAuthenticationService.SELF_LINK))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);

        FailureResponse failureResponse = sender.sendAndWaitFailure(requestOp);
        assertNotNull(failureResponse.failure);

        TestRequestSender.clearAuthToken();
        host.log("Verification of invalid token for Basic auth fails as expected");
    }

    @Test
    public void testVerificationValidAuthServiceToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, new TestAuthenticationService());
        host.log("Testing verification of valid token for external auth");

        TestRequestSender sender = new TestRequestSender(host);
        TestRequestSender.setAuthToken(TestAuthenticationService.ACCESS_TOKEN);

        // make a request to verification service
        Operation requestOp = Operation.createPost(
                UriUtils.buildUri(host, TestAuthenticationService.SELF_LINK))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);

        Operation responseOp = sender.sendAndWait(requestOp);
        Claims claims = responseOp.getBody(Claims.class);
        assertNotNull(claims);

        TestRequestSender.clearAuthToken();
        host.log("Verification of valid token for external auth succeeded");
    }

    @Test
    public void testVerificationInvalidAuthServiceToken() throws Throwable {
        VerificationHost host = createAndStartHost(true, new TestAuthenticationService());
        host.log("Testing verification of invalid token for external auth");

        // invalid accesstoken
        String invalidAccessToken = "aasfsfsf";
        TestRequestSender sender = new TestRequestSender(host);
        TestRequestSender.setAuthToken(invalidAccessToken);

        // make a request to verification service
        Operation requestOp = Operation.createPost(
                UriUtils.buildUri(host, TestAuthenticationService.SELF_LINK))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);;

        FailureResponse failureResponse = sender.sendAndWaitFailure(requestOp);
        assertNotNull(failureResponse.failure);

        TestRequestSender.clearAuthToken();
        host.log("Verification of invalid token for external auth fails as expected");
    }

    @After
    public void tearDown() {
        this.hostsToCleanup.forEach(VerificationHost::tearDown);
        this.hostsToCleanup.clear();
    }
}
