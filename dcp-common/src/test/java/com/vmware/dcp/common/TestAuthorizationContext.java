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

package com.vmware.dcp.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmware.dcp.common.Operation.AuthorizationContext;
import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.test.VerificationHost;
import com.vmware.dcp.services.common.GuestUserService;
import com.vmware.dcp.services.common.QueryTask.Query;
import com.vmware.dcp.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.dcp.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.dcp.services.common.RoleService.Policy;
import com.vmware.dcp.services.common.RoleService.RoleState;
import com.vmware.dcp.services.common.ServiceUriPaths;
import com.vmware.dcp.services.common.UserGroupService.UserGroupState;
import com.vmware.dcp.services.common.UserService.UserState;
import com.vmware.dcp.services.common.authn.AuthenticationConstants;

public class TestAuthorizationContext extends BasicTestCase {

    @Override
    public void beforeHostStart(VerificationHost host) {
        host.setAuthorizationEnabled(true);
    }

    public static class ClaimsVerificationService extends StatelessService {
        public static final String SELF_LINK = "/claims-verification";

        @Override
        public void handleStart(Operation op) {
            if (!verifyNewOperationHasSubject(op)) {
                return;
            }

            op.complete();
        }

        @Override
        public void handleGet(Operation op) {
            if (!verifyNewOperationHasSubject(op)) {
                return;
            }

            op.complete();
        }

        private boolean verifyNewOperationHasSubject(Operation op) {
            Map<String, String> map = UriUtils.parseUriQueryParams(op.getUri());
            String expectedSubject = map.get("subject");

            Operation newOp = Operation.createGet(this, "/not-important");
            AuthorizationContext ctx = newOp.getAuthorizationContext();
            if (ctx == null) {
                op.fail(new IllegalStateException("ctx == null"));
                return false;
            }

            Claims claims = ctx.getClaims();
            if (claims == null) {
                op.fail(new IllegalStateException("claims == null"));
                return false;
            }

            String actualSubject = claims.getSubject();
            if (!expectedSubject.equals(actualSubject)) {
                op.fail(new IllegalStateException("subject mismatch"));
                return false;
            }

            Map<String, String> actualProperties = claims.getProperties();
            if (actualProperties.size() == 0) {
                op.fail(new IllegalStateException("properties empty"));
                return false;
            }

            return true;
        }
    }

    AuthorizationContext createAuthorizationContext(String subject, VerificationHost host)
            throws GeneralSecurityException {
        Map<String, String> properties = new HashMap<>();
        properties.put("hello", "world");

        Claims.Builder builder = new Claims.Builder();
        builder.setIssuer(AuthenticationConstants.JWT_ISSUER);
        builder.setSubject(UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, subject));
        builder.setExpirationTime(Utils.getNowMicrosUtc() + TimeUnit.HOURS.toMicros(1));
        builder.setProperties(properties);

        Claims claims = builder.getResult();
        AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
        ab.setClaims(claims);
        ab.setToken(host.getTokenSigner().sign(claims));

        return ab.getResult();
    }

    @Test
    public void testPropagation() throws Throwable {
        String user = "unnamed-user@test.com";
        AuthorizationContext ctx = createAuthorizationContext(user, this.host);
        provisionUser(user, ClaimsVerificationService.SELF_LINK);

        URI claimsUri = UriUtils.buildUri(this.host, ClaimsVerificationService.SELF_LINK);
        claimsUri = UriUtils.extendUriWithQuery(claimsUri, "subject", ctx.getClaims().getSubject());

        // Through starting the claims verification service, we verify that the start operation's
        // authorization context is flowed to new operations created from within the start handler.
        Operation startOp = Operation.createPost(claimsUri);
        startOp.setAuthorizationContext(ctx);
        startOp.setCompletion(this.host.getCompletion());
        this.host.testStart(1);
        this.host.startService(startOp, new ClaimsVerificationService());
        this.host.testWait();

        // Make an internal request, forcing remote to make sure it goes over the wire (localhost).
        // This tests that the authorization context is propagated to remote peers if the
        // original outbound operation has an associated authorization context.
        Operation getOp = Operation.createGet(claimsUri);
        getOp.setAuthorizationContext(ctx);
        getOp.setCompletion(this.host.getCompletion());
        getOp.forceRemote();
        this.host.testStart(1);
        this.host.send(getOp);
        this.host.testWait();
    }

    public static class SetAuthorizationContextTestService extends StatelessService {
        public static final String SELF_LINK = "/set-authorization-context-test";
        public static final String EXPECT_USER_CONTEXT = "expectUserContext";

        @Override
        public void handleRequest(Operation op) {
            if (op.getAction() == Action.POST) {
                handleSetAuthorizationContext(op);
            } else if (op.getAction() == Action.GET) {
                handleGetAuthorizationContext(op);
            } else {
                op.fail(new IllegalArgumentException());
                return;
            }
        }

        private void handleSetAuthorizationContext(Operation op) {
            Claims claims = op.getBody(Claims.class);
            String token;

            // This signs an unchecked set of claims.
            // Never do this in production code...
            try {
                token = getTokenSigner().sign(claims);
            } catch (Exception e) {
                op.fail(e);
                return;
            }

            AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
            ab.setClaims(claims);
            ab.setToken(token);
            ab.setPropagateToClient(true);

            // Associate resulting authorization context with operation.
            setAuthorizationContext(op, ab.getResult());
            op.complete();
        }

        private void handleGetAuthorizationContext(Operation op) {
            AuthorizationContext ctx = op.getAuthorizationContext();
            if (ctx == null) {
                op.fail(new IllegalStateException("ctx == null"));
                return;
            }

            Claims claims = ctx.getClaims();
            if (claims == null) {
                op.fail(new IllegalStateException("claims == null"));
                return;
            }

            Map<String, String> params = UriUtils.parseUriQueryParams(op.getUri());
            String expectContext = params.get(EXPECT_USER_CONTEXT);
            if (expectContext.equals(Boolean.toString(true))) {
                assertNotEquals(GuestUserService.SELF_LINK, claims.getSubject());
            } else {
                assertEquals(GuestUserService.SELF_LINK, claims.getSubject());
            }

            op.setBody(claims).complete();
        }
    }

    @Test
    public void internalAuthorizationContextSetsCookie() throws Throwable {
        this.host.addPrivilegedService(SetAuthorizationContextTestService.class);
        this.host.startServiceAndWait(SetAuthorizationContextTestService.class,
                SetAuthorizationContextTestService.SELF_LINK);

        String user = "test-subject@test.com";
        provisionUser(user, SetAuthorizationContextTestService.SELF_LINK);

        Claims.Builder builder = new Claims.Builder();
        builder.setSubject(UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, user));
        builder.setExpirationTime(Utils.getNowMicrosUtc() + TimeUnit.HOURS.toMicros(1));
        Claims expected = builder.getResult();

        // Post to get a cookie
        URI postUri = UriUtils.buildUri(this.host, SetAuthorizationContextTestService.SELF_LINK);
        this.host.testStart(1);
        this.host.send(
                Operation.createPost(postUri).setBody(expected)
                        .setCompletion(this.host.getCompletion()).forceRemote());
        this.host.testWait();

        // Get to check the context was picked up from the cookie
        URI getUri = UriUtils.extendUriWithQuery(
                UriUtils.buildUri(this.host, SetAuthorizationContextTestService.SELF_LINK),
                SetAuthorizationContextTestService.EXPECT_USER_CONTEXT, "true");
        this.host.testStart(1);
        this.host.send(Operation
                .createGet(getUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    Claims actual = o.getBody(Claims.class);
                    if (!expected.getSubject().equals(actual.getSubject())) {
                        this.host.failIteration(new IllegalStateException("subject mismatch"));
                        return;
                    }

                    this.host.completeIteration();
                })
                .forceRemote());
        this.host.testWait();

    }

    @Test
    public void testExpiredAuthorizationContext() throws Throwable {
        this.host.addPrivilegedService(SetAuthorizationContextTestService.class);
        this.host.startServiceAndWait(SetAuthorizationContextTestService.class,
                SetAuthorizationContextTestService.SELF_LINK);

        String user = "test-subject@test.com";
        provisionUser(user, SetAuthorizationContextTestService.SELF_LINK);

        Claims.Builder builder = new Claims.Builder();
        builder.setSubject("test-subject");
        builder.setExpirationTime(new Long(0));
        Claims expected = builder.getResult();

        // Post to create an expired auth context
        URI postUri = UriUtils.buildUri(this.host, SetAuthorizationContextTestService.SELF_LINK);
        this.host.testStart(1);
        this.host.send(
                Operation.createPost(postUri).setBody(expected)
                        .setCompletion(this.host.getCompletion()).forceRemote());
        this.host.testWait();

        // Get should not see an auth context
        URI getUri = UriUtils.extendUriWithQuery(
                UriUtils.buildUri(this.host, SetAuthorizationContextTestService.SELF_LINK),
                SetAuthorizationContextTestService.EXPECT_USER_CONTEXT, "false");
        this.host.testStart(1);
        this.host.send(Operation
                .createGet(getUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                })
                .forceRemote());
        this.host.testWait();
    }

    public static class WhitelistAuthorizationContextTestService extends StatelessService {
        public static final String SELF_LINK = "/whitelist-authorization-context-test";

        @Override
        public void handleGet(Operation op) {
            if (!testWhitelistedFunctions(op)) {
                return;
            }

            op.complete();
        }

        private boolean testWhitelistedFunctions(Operation op) {
            try {
                AuthorizationContext ctx = this.getSystemAuthorizationContext();
                this.setAuthorizationContext(op, ctx);

                this.getTokenSigner();
            } catch (Exception e) {
                op.fail(e);
                return false;
            }

            return true;
        }
    }

    @Test
    public void privilegedServiceAuthContextCheck() throws Throwable {
        this.host.startServiceAndWait(WhitelistAuthorizationContextTestService.class,
                WhitelistAuthorizationContextTestService.SELF_LINK);
        URI testUri = UriUtils.buildUri(this.host,
                WhitelistAuthorizationContextTestService.SELF_LINK);

        // Make a call to test auth context functions without whitelisting the
        // service
        this.host.testStart(1);
        this.host.send(Operation
                .createGet(testUri)
                .setCompletion((o, e) -> {
                    // We should have failed
                    if (e == null) {
                        this.host.failIteration(
                                new IllegalStateException(
                                        "Whitelist functions failed to throw exception"));
                        return;
                    }

                    this.host.completeIteration();
                })
                .forceRemote());
        this.host.testWait();

        this.host.addPrivilegedService(WhitelistAuthorizationContextTestService.class);

        // Make a call to test auth context functions after whitelisting the
        // service
        this.host.testStart(1);
        this.host.send(Operation
                .createGet(testUri)
                .setCompletion((o, e) -> {
                    // We should have failed
                    if (e != null) {
                        this.host
                                .failIteration(
                                        new IllegalStateException(
                                                "Whitelist functions threw an exception on whitelisted service"));
                        return;
                    }

                    this.host.completeIteration();
                })
                .forceRemote());
        this.host.testWait();
    }

    private void provisionUser(String user, String serviceLink) throws Throwable {
        UserState userState = new UserState();
        userState.email = user;
        userState.documentSelfLink = user;
        UserGroupState userGroupState = new UserGroupState();
        userGroupState.documentSelfLink = user + "-user-group";
        userGroupState.query = new Query();
        userGroupState.query.setTermPropertyName("email");
        userGroupState.query.setTermMatchType(MatchType.TERM);
        userGroupState.query.setTermMatchValue(userState.email);
        ResourceGroupState resourceGroupState = new ResourceGroupState();
        resourceGroupState.documentSelfLink = user + "-resource-group";
        resourceGroupState.query = new Query();

        Query kindClause = new Query();
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK);
        kindClause.setTermMatchValue(serviceLink);
        kindClause.setTermMatchType(MatchType.TERM);
        resourceGroupState.query.addBooleanClause(kindClause);

        RoleState roleState = new RoleState();
        roleState.userGroupLink = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USER_GROUPS,
                userGroupState.documentSelfLink);
        roleState.resourceGroupLink = UriUtils.buildUriPath(
                ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS, resourceGroupState.documentSelfLink);
        roleState.verbs = new HashSet<>();
        roleState.verbs.add(Action.GET);
        roleState.verbs.add(Action.POST);
        roleState.policy = Policy.ALLOW;

        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        URI postUserUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USERS);
        this.host.testStart(4);
        this.host.send(Operation
                .createPost(postUserUri)
                .setBody(userState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    this.host.completeIteration();
                }));

        this.host.send(Operation
                .createPost(
                        UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USER_GROUPS))
                .setBody(userGroupState)
                .setCompletion(this.host.getCompletion()));

        this.host.send(Operation
                .createPost(
                        UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS))
                .setBody(resourceGroupState)
                .setCompletion(this.host.getCompletion()));

        this.host.send(Operation
                .createPost(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_ROLES))
                .setBody(roleState)
                .setCompletion(this.host.getCompletion()));

        this.host.testWait();
        OperationContext.setAuthorizationContext(null);
    }

    @Test
    public void testOperationJoin() throws Throwable {
        // create two services with two different auth contexts
        String user1 = "user1@test.com";
        String serviceName1 = "claims-1";
        AuthorizationContext ctx1 = createAuthorizationContext(user1, this.host);
        provisionUser(user1, serviceName1);

        String user2 = "user2@test.com";
        String serviceName2 = "claims-2";
        AuthorizationContext ctx2 = createAuthorizationContext(user2, this.host);
        provisionUser(user2, serviceName2);

        URI claimsUri1 = UriUtils.buildUri(this.host, serviceName1);
        claimsUri1 = UriUtils.extendUriWithQuery(claimsUri1, "subject", ctx1.getClaims()
                .getSubject());

        URI claimsUri2 = UriUtils.buildUri(this.host, serviceName2);
        claimsUri2 = UriUtils.extendUriWithQuery(claimsUri2, "subject", ctx2.getClaims()
                .getSubject());

        CompletionHandler handler = (o, ex) -> {
            if (ex != null) {
                this.host.failIteration(ex);
            }
            Map<String, String> map = UriUtils.parseUriQueryParams(o.getUri());
            String expectedSubject = map.get("subject");
            if (OperationContext.getAuthorizationContext() == null) {
                this.host.failIteration(new IllegalStateException("auth context is null"));
            }
            if (OperationContext.getAuthorizationContext().getClaims().getSubject()
                    .equals(expectedSubject)) {
                this.host.completeIteration();
                return;
            }
            this.host.failIteration(new IllegalStateException("subject mismatch"));
        };

        Operation startOp1 = Operation.createPost(claimsUri1);
        startOp1.setAuthorizationContext(ctx1);
        startOp1.setCompletion(handler);

        Operation startOp2 = Operation.createPost(claimsUri2);
        startOp2.setAuthorizationContext(ctx2);
        startOp2.setCompletion(handler);
        this.host.testStart(2);
        this.host.startService(startOp1, new ClaimsVerificationService());
        this.host.startService(startOp2, new ClaimsVerificationService());
        this.host.testWait();

        Operation getOp1 = Operation.createGet(claimsUri1).setCompletion(handler)
                .setReferer(this.host.getReferer());
        getOp1.setAuthorizationContext(ctx1);
        Operation getOp2 = Operation.createGet(claimsUri2).setCompletion(handler)
                .setReferer(this.host.getReferer());
        getOp2.setAuthorizationContext(ctx2);
        host.setSystemAuthorizationContext();
        OperationJoin joinOp = OperationJoin.create(getOp1, getOp2);
        this.host.testStart(2);
        joinOp.sendWith(this.host);
        this.host.testWait();

        // create OperationJon with a joined completion handler. The handler should
        // be invoked as the system user
        OperationJoin joinOpWithHandler = OperationJoin.create(getOp1, getOp2);
        JoinedCompletionHandler joinHandler = (ops, exc) -> {
            if (OperationContext.getAuthorizationContext() == null) {
                this.host.failIteration(new IllegalStateException("auth context is null"));
            }
            if (OperationContext.getAuthorizationContext().getClaims().getSubject()
                    .equals(ServiceUriPaths.CORE_AUTHZ_SYSTEM_USER)) {
                this.host.completeIteration();
                return;
            }
            this.host.failIteration(new IllegalStateException("subject mismatch"));
        };
        joinOpWithHandler.setCompletion(joinHandler);
        this.host.testStart(1);
        joinOpWithHandler.sendWith(this.host);
        this.host.testWait();
        host.resetSystemAuthorizationContext();
    }
}
