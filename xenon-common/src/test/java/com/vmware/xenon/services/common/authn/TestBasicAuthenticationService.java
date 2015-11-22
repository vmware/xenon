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

package com.vmware.xenon.services.common.authn;

import java.net.URI;
import java.util.Base64;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.http.netty.CookieJar;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthCredentialsFactoryService;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;
import com.vmware.xenon.services.common.UserFactoryService;
import com.vmware.xenon.services.common.UserService.UserState;
import com.vmware.xenon.services.common.authn.AuthenticationRequest.AuthenticationRequestType;

public class TestBasicAuthenticationService {

    private VerificationHost host;
    private static final String USER = "jane@doe.com";
    private static final String INVALID_USER = "janedoe@doe.com";
    private static final String PASSWORD = "password-for-jane";
    private static final String INVALID_PASSWORD = "invalid-password";
    private static final String BASIC_AUTH_PREFIX = "Basic ";
    private static final String BASIC_AUTH_USER_SEPERATOR = ":";
    private static final String SET_COOKIE_HEADER = "Set-Cookie";

    @Before
    public void setUp() throws Exception {
        this.host = VerificationHost.create(0, null);
        try {
            this.host.setAuthorizationEnabled(true);
            this.host.start();
            this.host.setSystemAuthorizationContext();
            this.host.waitForServiceAvailable(AuthCredentialsFactoryService.SELF_LINK);
            this.host.waitForServiceAvailable(BasicAuthenticationService.SELF_LINK);
            this.host.waitForServiceAvailable(UserFactoryService.SELF_LINK);

            // initialize users
            UserState state = new UserState();
            state.email = USER;
            AuthCredentialsServiceState authServiceState = new AuthCredentialsServiceState();
            authServiceState.userEmail = USER;
            authServiceState.privateKey = PASSWORD;

            URI userUri = UriUtils.buildUri(this.host, UserFactoryService.SELF_LINK);
            Operation userOp = Operation.createPost(userUri)
                    .setBody(state)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        this.host.completeIteration();
                    });
            URI authUri = UriUtils.buildUri(this.host, AuthCredentialsFactoryService.SELF_LINK);
            Operation authOp = Operation.createPost(authUri)
                    .setBody(authServiceState)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        this.host.completeIteration();
                    });
            this.host.testStart(2);
            this.host.send(userOp);
            this.host.send(authOp);
            this.host.testWait();
            this.host.resetAuthorizationContext();
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @After
    public void tearDown() throws InterruptedException {
        this.host.tearDown();
    }

    @Test
    public void testAuth() throws Throwable {

        URI authServiceUri = UriUtils.buildUri(this.host, BasicAuthenticationService.SELF_LINK);
        // send a request with no authentication header
        this.host.testStart(1);
        this.host
                .send(Operation
                        .createPost(authServiceUri)
                        .setBody(new Object())
                        .setCompletion(
                                (o, e) -> {
                                    if (e == null) {
                                        this.host.failIteration(new IllegalStateException(
                                                "request should have failed"));
                                        return;
                                    }
                                    if (o.getStatusCode() != Operation.STATUS_CODE_UNAUTHORIZED) {
                                        this.host.failIteration(new IllegalStateException(
                                                "Invalid status code returned"));
                                        return;
                                    }
                                    String authHeader = o
                                            .getResponseHeaders()
                                            .get(
                                                    BasicAuthenticationService.WWW_AUTHENTICATE_HEADER_NAME);
                                    if (authHeader == null
                                            || !authHeader
                                                    .equals(BasicAuthenticationService.WWW_AUTHENTICATE_HEADER_VALUE)) {
                                        this.host.failIteration(new IllegalStateException(
                                                "Invalid status code returned"));
                                        return;
                                    }
                                    this.host.completeIteration();
                                }));
        this.host.testWait();

        // send a request with an authentication header for an invalid user
        String userPassStr = new String(Base64.getEncoder().encode(
                new StringBuffer(INVALID_USER).append(BASIC_AUTH_USER_SEPERATOR).append(PASSWORD)
                        .toString().getBytes()));
        String headerVal = new StringBuffer("Basic ").append(userPassStr).toString();
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(BasicAuthenticationService.AUTHORIZATION_HEADER_NAME, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e == null) {
                                this.host.failIteration(
                                        new IllegalStateException("request should have failed"));
                                return;
                            }
                            if (o.getStatusCode() != Operation.STATUS_CODE_FORBIDDEN) {
                                this.host.failIteration(new IllegalStateException(
                                        "Invalid status code returned"));
                                return;
                            }
                            this.host.completeIteration();
                        }));
        this.host.testWait();

        // send a request with a malformed authentication header
        userPassStr = new String(Base64.getEncoder().encode(
                new StringBuffer(USER).toString().getBytes()));
        headerVal = new StringBuffer(BASIC_AUTH_PREFIX).append(userPassStr).toString();
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(BasicAuthenticationService.AUTHORIZATION_HEADER_NAME, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e == null) {
                                this.host.failIteration(
                                        new IllegalStateException("request should have failed"));
                                return;
                            }
                            if (o.getStatusCode() != Operation.STATUS_CODE_BAD_REQUEST) {
                                this.host.failIteration(new IllegalStateException(
                                        "Invalid status code returned"));
                                return;
                            }
                            this.host.completeIteration();
                        }));
        this.host.testWait();

        // send a request with an invalid password
        userPassStr = new String(Base64.getEncoder().encode(
                new StringBuffer(USER).append(BASIC_AUTH_USER_SEPERATOR).append(INVALID_PASSWORD)
                        .toString().getBytes()));
        headerVal = new StringBuffer(BASIC_AUTH_PREFIX).append(userPassStr).toString();
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(BasicAuthenticationService.AUTHORIZATION_HEADER_NAME, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e == null) {
                                this.host.failIteration(
                                        new IllegalStateException("request should have failed"));
                                return;
                            }
                            if (o.getStatusCode() != Operation.STATUS_CODE_FORBIDDEN) {
                                this.host.failIteration(new IllegalStateException(
                                        "Invalid status code returned"));
                                return;
                            }
                            this.host.completeIteration();
                        }));
        this.host.testWait();

        // finally send a valid request
        userPassStr = new String(Base64.getEncoder().encode(
                new StringBuffer(USER).append(BASIC_AUTH_USER_SEPERATOR).append(PASSWORD)
                        .toString().getBytes()));
        headerVal = new StringBuffer(BASIC_AUTH_PREFIX).append(userPassStr).toString();
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(BasicAuthenticationService.AUTHORIZATION_HEADER_NAME, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }
                            if (o.getStatusCode() != Operation.STATUS_CODE_OK) {
                                this.host.failIteration(new IllegalStateException(
                                        "Invalid status code returned"));
                                return;
                            }
                            if (o.getAuthorizationContext() == null) {
                                this.host.failIteration(new IllegalStateException(
                                        "Authorization context not set"));
                                return;
                            }
                            // now issue a logout
                        AuthenticationRequest request = new AuthenticationRequest();
                        request.requestType = AuthenticationRequestType.LOGOUT;
                        Operation logoutOp = Operation
                                .createPost(authServiceUri)
                                .setBody(request)
                                .forceRemote()
                                .setCompletion(
                                        (oo, ee) -> {
                                            if (ee != null) {
                                                this.host.failIteration(ee);
                                                return;
                                            }
                                            if (oo.getStatusCode() != Operation.STATUS_CODE_OK) {
                                                this.host.failIteration(new IllegalStateException(
                                                        "Invalid status code returned"));
                                                return;
                                            }
                                            String cookieHeader = oo.getResponseHeader(SET_COOKIE_HEADER);
                                            if (cookieHeader == null) {
                                                this.host.failIteration(new IllegalStateException(
                                                        "Cookie is null"));
                                            }
                                            Map<String, String> cookieElements = CookieJar.decodeCookies(cookieHeader);
                                            if (!cookieElements.get("Max-Age").equals("0")) {
                                                this.host.failIteration(new IllegalStateException(
                                                        "Max-Age for cookie is not zero"));
                                            }
                                            this.host.resetAuthorizationContext();
                                            this.host.completeIteration();
                                        });
                        this.host.setAuthorizationContext(o.getAuthorizationContext());
                        this.host.send(logoutOp);
                    }));
        this.host.testWait();
    }

}
