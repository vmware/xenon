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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.http.netty.CookieJar;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthCredentialsService;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;
import com.vmware.xenon.services.common.UserService;
import com.vmware.xenon.services.common.UserService.UserState;
import com.vmware.xenon.services.common.authn.AuthenticationRequest.AuthenticationRequestType;

public class TestBasicAuthenticationService extends BasicTestCase {
    private static final String USER = "jane@doe.com";
    private static final String INVALID_USER = "janedoe@doe.com";
    private static final String PASSWORD = "password-for-jane";
    private static final String INVALID_PASSWORD = "invalid-password";
    private static final String BASIC_AUTH_PREFIX = "Basic ";
    private static final String BASIC_AUTH_USER_SEPARATOR = ":";
    private static final String SET_COOKIE_HEADER = "Set-Cookie";

    private String credentialsServiceStateSelfLink;

    @Override
    public void beforeHostStart(VerificationHost h) {
        h.setAuthorizationEnabled(true);
    }

    @Before
    public void setUp() throws Exception {
        try {
            this.host.setSystemAuthorizationContext();
            this.host.waitForServiceAvailable(AuthCredentialsService.FACTORY_LINK);
            this.host.waitForServiceAvailable(BasicAuthenticationService.SELF_LINK);
            this.host.waitForServiceAvailable(UserService.FACTORY_LINK);

            // initialize users
            UserState state = new UserState();
            state.email = USER;
            AuthCredentialsServiceState authServiceState = new AuthCredentialsServiceState();
            authServiceState.userEmail = USER;
            authServiceState.privateKey = PASSWORD;

            URI userUri = UriUtils.buildUri(this.host, UserService.FACTORY_LINK);
            Operation userOp = Operation.createPost(userUri)
                    .setBody(state)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        this.host.completeIteration();
                    });
            URI authUri = UriUtils.buildUri(this.host, AuthCredentialsService.FACTORY_LINK);
            Operation authOp = Operation.createPost(authUri)
                    .setBody(authServiceState)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        this.credentialsServiceStateSelfLink = o.getBody(AuthCredentialsServiceState.class).documentSelfLink;
                        this.host.completeIteration();
                    });
            this.host.testStart(2);
            this.host.send(userOp);
            this.host.send(authOp);
            this.host.testWait();
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @Test
    public void testAuth() throws Throwable {
        this.host.resetAuthorizationContext();
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
                                            .getResponseHeader(
                                                    BasicAuthenticationUtils.WWW_AUTHENTICATE_HEADER_NAME);
                                    if (authHeader == null
                                            || !authHeader
                                                    .equals(BasicAuthenticationUtils.WWW_AUTHENTICATE_HEADER_VALUE)) {
                                        this.host.failIteration(new IllegalStateException(
                                                "Invalid status code returned"));
                                        return;
                                    }
                                    this.host.completeIteration();
                                }));
        this.host.testWait();

        // send a request with an authentication header for an invalid user
        String userPassStr = new String(Base64.getEncoder().encode(
                new StringBuffer(INVALID_USER).append(BASIC_AUTH_USER_SEPARATOR).append(PASSWORD)
                        .toString().getBytes()));
        String headerVal = new StringBuffer("Basic ").append(userPassStr).toString();
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
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
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
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
                new StringBuffer(USER).append(BASIC_AUTH_USER_SEPARATOR).append(INVALID_PASSWORD)
                        .toString().getBytes()));
        headerVal = new StringBuffer(BASIC_AUTH_PREFIX).append(userPassStr).toString();
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
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

        // Next send a valid request
        userPassStr = new String(Base64.getEncoder().encode(
                new StringBuffer(USER).append(BASIC_AUTH_USER_SEPARATOR).append(PASSWORD)
                        .toString().getBytes()));
        headerVal = new StringBuffer(BASIC_AUTH_PREFIX).append(userPassStr).toString();
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
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

        // Finally, send a valid remote request, and validate the cookie & auth token
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .forceRemote()
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
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
                            if (!validateAuthToken(o)) {
                                return;
                            }
                            this.host.completeIteration();
                        }));
        this.host.testWait();

    }

    @Test
    public void testAuthExpiration() throws Throwable {
        this.host.resetAuthorizationContext();
        URI authServiceUri = UriUtils.buildUri(this.host, BasicAuthenticationService.SELF_LINK);

        // Next send a valid request
        String userPassStr = Base64.getEncoder().encodeToString(
                (USER + BASIC_AUTH_USER_SEPARATOR + PASSWORD).getBytes());
        String headerVal = BASIC_AUTH_PREFIX + userPassStr;

        long oneHourFromNowBeforeAuth = Utils.getNowMicrosUtc() + TimeUnit.HOURS.toMicros(1);

        // do not specify expiration
        AuthenticationRequest authReq = new AuthenticationRequest();

        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(authReq)
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            long oneHourFromNowAfterAuth =
                                    Utils.getNowMicrosUtc() + TimeUnit.HOURS.toMicros(1);

                            // default expiration(1hour) must be used
                            validateExpirationTimeRange(o.getAuthorizationContext(),
                                    oneHourFromNowBeforeAuth, oneHourFromNowAfterAuth);

                            this.host.completeIteration();
                        }));
        this.host.testWait();

        // set expiration 1min
        authReq = new AuthenticationRequest();
        authReq.sessionExpirationSeconds = 60L;

        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(authReq)
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            long tenMinAfterNowInMicro =
                                    Utils.getNowMicrosUtc() + TimeUnit.MINUTES.toMicros(10);

                            // expiration has set to 1min, so it must be before now + 10min
                            validateExpirationTimeRange(o.getAuthorizationContext(),
                                    null, tenMinAfterNowInMicro);

                            this.host.completeIteration();
                        }));
        this.host.testWait();

        // with negative sec
        authReq = new AuthenticationRequest();
        authReq.sessionExpirationSeconds = -1L;

        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(authReq)
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            // must be before now
                            validateExpirationTimeRange(o.getAuthorizationContext(),
                                    null, Utils.getNowMicrosUtc());

                            this.host.completeIteration();
                        }));
        this.host.testWait();

        // with 0
        authReq = new AuthenticationRequest();
        authReq.sessionExpirationSeconds = 0L;

        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(authReq)
                .addRequestHeader(BasicAuthenticationUtils.AUTHORIZATION_HEADER_NAME, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            // must be before now
                            validateExpirationTimeRange(o.getAuthorizationContext(),
                                    null, Utils.getNowMicrosUtc());

                            this.host.completeIteration();
                        }));
        this.host.testWait();

    }

    private void validateExpirationTimeRange(AuthorizationContext authContext, Long fromInMicro,
            Long toInMicro) {
        assertNotNull(authContext);
        assertNotNull(authContext.getClaims());
        assertNotNull(authContext.getClaims().getExpirationTime());
        long expirationInMicro = authContext.getClaims().getExpirationTime();

        if (fromInMicro != null && expirationInMicro <= fromInMicro) {
            String msg = String.format("expiration must be greater than %d but was %d", fromInMicro,
                    expirationInMicro);
            this.host.failIteration(new IllegalStateException(msg));
        }

        if (toInMicro != null && toInMicro <= expirationInMicro) {
            String msg = String.format("expiration must be greater less %d but was %d", toInMicro,
                    expirationInMicro);
            this.host.failIteration(new IllegalStateException(msg));
        }

    }

    @Test
    public void testCustomProperties() throws Throwable {
        String firstProperty = "Property1";
        String firstValue = "Value1";
        String secondProperty = "Property2";
        String secondValue = "Value2";
        String updatedValue = "UpdatedValue";

        // add custom property
        URI authUri = UriUtils.buildUri(this.host, this.credentialsServiceStateSelfLink);
        AuthCredentialsServiceState authServiceState = new AuthCredentialsServiceState();
        Map<String, String> customProperties = new HashMap<String, String>();
        customProperties.put(firstProperty, firstValue);
        authServiceState.customProperties = customProperties;
        Operation addProperty = Operation.createPatch(authUri)
                .setBody(authServiceState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    AuthCredentialsServiceState state = o.getBody(AuthCredentialsServiceState.class);
                    assertEquals("There should be only one custom property", state.customProperties.size(), 1);
                    assertEquals(state.customProperties.get(firstProperty), firstValue);
                    this.host.completeIteration();
                });
        this.host.testStart(1);
        this.host.send(addProperty);
        this.host.testWait();

        // update custom properties

        customProperties.put(firstProperty, updatedValue);
        customProperties.put(secondProperty, secondValue);
        authServiceState.customProperties = customProperties;
        Operation updateProperies = Operation.createPatch(authUri)
                .setBody(authServiceState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    AuthCredentialsServiceState state = o.getBody(AuthCredentialsServiceState.class);
                    assertEquals("There should be two custom properties", state.customProperties.size(), 2);
                    assertEquals(state.customProperties.get(firstProperty), updatedValue);
                    assertEquals(state.customProperties.get(secondProperty), secondValue);
                    this.host.completeIteration();
                });
        this.host.testStart(1);
        this.host.send(updateProperies);
        this.host.testWait();
    }

    @Test
    public void testAuthWithUserInfo() throws Throwable {
        this.host.resetAuthorizationContext();
        String userPassStr = new StringBuilder(USER).append(BASIC_AUTH_USER_SEPARATOR).append(PASSWORD)
                        .toString();
        URI authServiceUri = UriUtils.buildUri(this.host, BasicAuthenticationService.SELF_LINK, null, userPassStr);

        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
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

    private boolean validateAuthToken(Operation op) {
        String cookieHeader = op.getResponseHeader(SET_COOKIE_HEADER);
        if (cookieHeader == null) {
            this.host.failIteration(new IllegalStateException("Missing cookie header"));
            return false;
        }

        Map<String, String> cookieElements = CookieJar.decodeCookies(cookieHeader);
        if (!cookieElements.containsKey(AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE)) {
            this.host.failIteration(new IllegalStateException("Missing auth cookie"));
            return false;
        }

        if (op.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER) == null) {
            this.host.failIteration(new IllegalStateException("Missing auth token"));
            return false;
        }

        String authCookie = cookieElements.get(AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE);
        String authToken = op.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER);

        if (!authCookie.equals(authToken)) {
            this.host.failIteration(new IllegalStateException("Auth token and auth cookie don't match"));
            return false;
        }
        return true;
    }
}
