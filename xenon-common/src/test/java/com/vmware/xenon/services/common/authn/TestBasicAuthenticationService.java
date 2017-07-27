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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import static com.vmware.xenon.services.common.authn.BasicAuthenticationUtils.constructBasicAuth;

import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceStateCollectionUpdateRequest;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthCredentialsService;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ServiceUriPaths;
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
    private static final String ROLE = "guest-role";
    private static final String USER_GROUP = "guest-user-group";
    private static final String RESOURCE_GROUP = "guest-resource-group";

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
            state.documentSelfLink = USER;
            AuthCredentialsServiceState authServiceState = new AuthCredentialsServiceState();
            authServiceState.userEmail = USER;
            authServiceState.privateKey = PASSWORD;
            EnumSet<Service.Action> verbs = EnumSet.of(Service.Action.GET, Service.Action.POST);

            this.host.testStart(1);
            AuthorizationSetupHelper.create()
                    .setHost(this.host)
                    .setUserEmail(USER)
                    .setUserPassword(PASSWORD)
                    .setUserSelfLink(UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, USER))
                    .setDocumentKind(Utils.buildKind(ExampleService.ExampleServiceState.class))
                    .setCredentialsSelfLink(UriUtils.buildUriPath(ServiceUriPaths.CORE_CREDENTIALS, USER))
                    .setIsAdmin(false)
                    .setDocumentLink(ExampleService.FACTORY_LINK)
                    .setCompletion(this.host.getCompletion())
                    .start();
            this.host.testWait();

            this.host.testStart(1);
            AuthorizationSetupHelper.create()
                    .setHost(this.host)
                    .setUserSelfLink(UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, USER))
                    .setDocumentLink(ExampleService.FACTORY_LINK)
                    .setDocumentKind(Utils.buildKind(ExampleService.ExampleServiceState.class))
                    .setUserGroupName(USER_GROUP)
                    .setResourceGroupName(RESOURCE_GROUP)
                    .setRoleName(ROLE)
                    .setVerbs(verbs)
                    .setCompletion(this.host.getCompletion())
                    .setupRole();
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
        String headerVal = constructBasicAuth(INVALID_USER, PASSWORD);
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
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
        String userPassStr = new String(Base64.getEncoder().encode(
                new StringBuffer(USER).toString().getBytes()));
        headerVal = new StringBuffer(BASIC_AUTH_PREFIX).append(userPassStr).toString();
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
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
        headerVal = constructBasicAuth(USER, INVALID_PASSWORD);
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
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
        headerVal = constructBasicAuth(USER, PASSWORD);
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(new Object())
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
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
                                                    return;
                                                }
                                                Cookie cookie = ClientCookieDecoder.LAX.decode(cookieHeader);
                                                if (cookie.maxAge() != 0) {
                                                    this.host.failIteration(new IllegalStateException(
                                                            "Max-Age for cookie is not zero"));
                                                    return;
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
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
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
        // delete the user and issue a remote request as the user
        // we should see a 200 response as xenon invokes this
        // request with the guest context
        this.host.setSystemAuthorizationContext();
        this.host.sendAndWait(
                Operation.createDelete(UriUtils.buildUri(this.host,
                        UriUtils.buildUriPath(UserService.FACTORY_LINK, USER)))
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }
                            this.host.completeIteration();
                        }));
        this.host.resetSystemAuthorizationContext();
        this.host.assumeIdentity(UriUtils.buildUriPath(UserService.FACTORY_LINK, USER));
        this.host.testStart(1);
        this.host.send(Operation
                .createGet(UriUtils.buildUri(this.host, UserService.FACTORY_LINK))
                .forceRemote()
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
                            this.host.completeIteration();
                        }));
        this.host.testWait();
    }

    @Test
    public void verifyLoginLogoutLoginWithTokenInHeader() {
        verifyLoginLogoutLogin(false);
    }

    @Test
    public void verifyLoginLogoutLoginWithTokenInCookie() {
        verifyLoginLogoutLogin(true);
    }

    public void verifyLoginLogoutLogin(boolean withCookie) {
        String[] authToken = new String[1];
        String[] oldAuthToken = new String[1];
        this.host.resetAuthorizationContext();
        URI authServiceUri = UriUtils.buildUri(this.host, BasicAuthenticationService.SELF_LINK);

        String headerVal;
        ExampleService.ExampleServiceState state = new ExampleService.ExampleServiceState();
        state.name = "Test";

        // First Login
        headerVal = constructBasicAuth(USER, PASSWORD);
        login(authServiceUri, headerVal, authToken, 1L);
        oldAuthToken[0] = authToken[0];

        // Verify token works
        Operation op1 = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
        addToken(op1, authToken[0], withCookie);
        this.host.sendAndWaitExpectSuccess(op1);

        // Verify that without valid token user operation fails.
        Operation op2 = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
        this.host.sendAndWaitExpectFailure(op2);

        // Logout and wait for token to expire
        logout(authServiceUri, authToken);
        this.host.waitFor("Token not expired", () -> {
            Operation o = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
            addToken(o, authToken[0], withCookie);
            Operation result = this.host.waitForResponse(o);
            return result.getStatusCode() == Operation.STATUS_CODE_FORBIDDEN;
        });

        // Login again and get new token
        login(authServiceUri, headerVal, authToken, 60L);
        assertNotEquals(authToken[0], oldAuthToken[0]);

        // Verify new token works
        op1 = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
        addToken(op1, authToken[0], withCookie);
        this.host.sendAndWaitExpectSuccess(op1);

        // Verify user operation fails with old expired auth token
        op1 = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
        addToken(op1, oldAuthToken[0], withCookie);
        this.host.sendAndWaitExpectFailure(op1);

        // Verify that without valid token user operation fails.
        op2 = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
        this.host.sendAndWaitExpectFailure(op2);
    }

    void addToken(Operation op, String token, boolean withCookie) {
        if (withCookie) {
            op.setCookies(Collections.singletonMap(AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE, token));
        } else {
            op.addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, token);
        }
    }

    private void logout(URI authServiceUri, String[] authToken) {
        this.host.testStart(1);
        AuthenticationRequest request = new AuthenticationRequest();
        request.requestType = AuthenticationRequestType.LOGOUT;
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(request)
                .forceRemote()
                .addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, authToken[0])
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
                                return;
                            }
                            Cookie cookie = ClientCookieDecoder.LAX.decode(cookieHeader);
                            if (cookie.maxAge() != 0) {
                                this.host.failIteration(new IllegalStateException(
                                        "Max-Age for cookie is not zero"));
                                return;
                            }

                            this.host.completeIteration();
                        }));
        this.host.testWait();
    }

    private void login(URI authServiceUri, String headerVal, String[] authToken, Long tokenExpiration) {
        AuthenticationRequest authReq = new AuthenticationRequest();
        authReq.sessionExpirationSeconds = tokenExpiration;
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(authReq)
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
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
                            authToken[0] = o.getAuthorizationContext().getToken();
                            this.host.completeIteration();
                        }));
        this.host.testWait();
    }

    @Test
    public void testAuthExpiration() throws Throwable {
        this.host.resetAuthorizationContext();
        URI authServiceUri = UriUtils.buildUri(this.host, BasicAuthenticationService.SELF_LINK);

        // Next send a valid request
        String headerVal = constructBasicAuth(USER, PASSWORD);

        long exp = Utils.fromNowMicrosUtc(TimeUnit.HOURS.toMicros(1));
        // round micros to the nearest second
        long oneHourFromNowBeforeAuth = TimeUnit.SECONDS.toMicros(TimeUnit.MICROSECONDS.toSeconds(exp));

        // do not specify expiration
        AuthenticationRequest authReq = new AuthenticationRequest();

        this.host.testStart(1);
        this.host.send(Operation
                .createPost(authServiceUri)
                .setBody(authReq)
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            long oneHourFromNowAfterAuth =
                                    Utils.getSystemNowMicrosUtc() + TimeUnit.HOURS.toMicros(1);

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
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            long tenMinAfterNowInMicro =
                                    Utils.getSystemNowMicrosUtc() + TimeUnit.MINUTES.toMicros(10);

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
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            // must be before now
                            validateExpirationTimeRange(o.getAuthorizationContext(),
                                    null, Utils.getSystemNowMicrosUtc());

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
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, headerVal)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            // must be before now
                            validateExpirationTimeRange(o.getAuthorizationContext(),
                                    null, Utils.getSystemNowMicrosUtc());

                            this.host.completeIteration();
                        }));
        this.host.testWait();

    }

    private void validateExpirationTimeRange(AuthorizationContext authContext, Long fromInMicro,
            Long toInMicro) {
        assertNotNull(authContext);
        assertNotNull(authContext.getClaims());
        assertNotNull(authContext.getClaims().getExpirationTime());
        long expirationInMicro = TimeUnit.SECONDS.toMicros(authContext.getClaims().getExpirationTime());

        if (fromInMicro != null && expirationInMicro < fromInMicro) {
            String msg = String.format("expiration must be greater than %d but was %d", fromInMicro,
                    expirationInMicro);
            this.host.failIteration(new IllegalStateException(msg));
        }

        if (toInMicro != null && toInMicro < expirationInMicro) {
            String msg = String.format("expiration must be greater less %d but was %d", toInMicro,
                    expirationInMicro);
            this.host.failIteration(new IllegalStateException(msg));
        }

    }

    @Test
    public void testCustomPropertiesAndTenantLinks() throws Throwable {
        String firstProperty = "Property1";
        String firstValue = "Value1";
        String secondProperty = "Property2";
        String secondValue = "Value2";
        String updatedValue = "UpdatedValue";

        // add custom property
        URI authUri = UriUtils.buildUri(this.host, UriUtils.buildUriPath(ServiceUriPaths.CORE_CREDENTIALS, USER));
        AuthCredentialsServiceState authServiceState = new AuthCredentialsServiceState();
        Map<String, String> customProperties = new HashMap<>();
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

        // update tenantLinks via a CollectionsPatchRequest
        List<String> tenantLinks = new ArrayList<>();
        tenantLinks.add("foo");
        Map<String, Collection<Object>> itemsToAdd = new HashMap<>();
        itemsToAdd.put("tenantLinks", new ArrayList<>(tenantLinks));
        ServiceStateCollectionUpdateRequest patchRequest = ServiceStateCollectionUpdateRequest.create(itemsToAdd, null);
        updateProperies = Operation.createPatch(authUri)
                .setBody(patchRequest)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    AuthCredentialsServiceState state = o.getBody(AuthCredentialsServiceState.class);
                    assertEquals("There should be one tenantLink", state.tenantLinks.size(), 1);
                    this.host.completeIteration();
                });
        this.host.testStart(1);
        this.host.send(updateProperies);
        this.host.testWait();

        // remove the tenantLink
        Map<String, Collection<Object>> itemsToRemove = new HashMap<>();
        itemsToRemove.put("tenantLinks", new ArrayList<>(tenantLinks));
        patchRequest = ServiceStateCollectionUpdateRequest.create(null, itemsToRemove);
        updateProperies = Operation.createPatch(authUri)
                .setBody(patchRequest)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    AuthCredentialsServiceState state = o.getBody(AuthCredentialsServiceState.class);
                    assertEquals("There should be no tenantLink", state.tenantLinks.size(), 0);
                    this.host.completeIteration();
                });
        this.host.testStart(1);
        this.host.send(updateProperies);
        this.host.testWait();
    }

    @Test
    public void testAuthWithUserInfo() throws Throwable {
        doTestAuthWithUserInfo(false);
        doTestAuthWithUserInfo(true);
    }

    private void doTestAuthWithUserInfo(boolean remote) throws Throwable {
        this.host.resetAuthorizationContext();
        String userPassStr = new StringBuilder(USER).append(BASIC_AUTH_USER_SEPARATOR).append(PASSWORD)
                .toString();
        URI authServiceUri = UriUtils.buildUri(this.host, BasicAuthenticationService.SELF_LINK, null, userPassStr);

        this.host.testStart(1);
        Operation post = Operation.createPost(authServiceUri)
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
                            if (!o.isRemote() && o.getAuthorizationContext() == null) {
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
                                                    return;
                                                }
                                                Cookie cookie = ClientCookieDecoder.LAX.decode(cookieHeader);
                                                if (cookie.maxAge() != 0) {
                                                    this.host.failIteration(new IllegalStateException(
                                                            "Max-Age for cookie is not zero"));
                                                    return;
                                                }
                                                if (!cookie.isHttpOnly()) {
                                                    this.host.failIteration(new IllegalStateException(
                                                            "Cookie is not HTTP-only"));
                                                    return;
                                                }
                                                this.host.resetAuthorizationContext();
                                                this.host.completeIteration();
                                            });
                            this.host.setAuthorizationContext(o.getAuthorizationContext());
                            this.host.send(logoutOp);
                        });

        if (remote) {
            post.forceRemote();
        }

        this.host.send(post);
        this.host.testWait();
    }

    private boolean validateAuthToken(Operation op) {
        String cookieHeader = op.getResponseHeader(SET_COOKIE_HEADER);
        if (cookieHeader == null) {
            this.host.failIteration(new IllegalStateException("Missing cookie header"));
            return false;
        }

        Cookie tokenCookie = ClientCookieDecoder.LAX.decode(cookieHeader);
        if (!AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE.equals(tokenCookie.name())) {
            this.host.failIteration(new IllegalStateException("Missing auth cookie"));
            return false;
        }

        if (op.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER) == null) {
            this.host.failIteration(new IllegalStateException("Missing auth token"));
            return false;
        }

        String authCookie = tokenCookie.value();
        String authToken = op.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER);

        if (!authCookie.equals(authToken)) {
            this.host.failIteration(new IllegalStateException("Auth token and auth cookie don't match"));
            return false;
        }
        return true;
    }
}
