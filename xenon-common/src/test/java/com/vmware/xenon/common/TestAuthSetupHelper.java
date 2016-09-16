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
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.EnumSet;
import java.util.UUID;

import org.junit.Test;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.http.netty.NettyHttpServiceClient;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.ResourceGroupService;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserGroupService;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;

// Note that we can't use BasicReusableHostTestCase here because we need to enable
// authorization on the host before it's started, and BasicReusableHostTestCase
// doesn't have authorization enabled.
public class TestAuthSetupHelper extends BasicTestCase {

    private static final String GUEST_ROLE = "guest-role";
    private static final String GUEST_USER_GROUP = "guest-user-group";
    private static final String GUEST_RESOURCE_GROUP = "guest-resource-group";

    @Override
    public void beforeHostStart(VerificationHost host) {
        // Enable authorization service; this is an end to end test
        host.setAuthorizationService(new AuthorizationContextService());
        host.setAuthorizationEnabled(true);
    }

    private String adminUser = "admim@localhost";
    private String exampleUser = "example@localhost";
    private String exampleWithManagementServiceUser = "exampleWithManagementService@localhost";

    /**
     * Validate the AuthorizationSetupHelper
     */
    @Test
    public void testAuthSetupHelper() throws Throwable {
        this.host.waitForServiceAvailable(ServiceHostManagementService.SELF_LINK);
        // Step 1: Set up two users, one admin, one not.
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        makeUsersWithAuthSetupHelper();

        AuthorizationHelper authHelper = new AuthorizationHelper(this.host);

        // Make sure the following tests don't automatically set the authorization
        // context: we explicitly set the auth token to make sure it's acting as
        // expected.
        OperationContext.setAuthorizationContext(null);

        // Step 2: Have each user login and get a token
        String adminAuthToken = authHelper.login(this.adminUser, this.adminUser);
        String exampleAuthToken = authHelper.login(this.exampleUser, this.exampleUser);
        String exampleWithMgmtAuthToken = authHelper.login(this.exampleWithManagementServiceUser,
                this.exampleWithManagementServiceUser);

        // Step 3: Have each user create an example document
        createExampleDocument(adminAuthToken);
        createExampleDocument(exampleAuthToken);

        // Step 4: Verify that the admin can see both documents, but the non-admin
        // can only see the one it created
        assertTrue(numberExampleDocuments(adminAuthToken) == 2);
        assertTrue(numberExampleDocuments(exampleAuthToken) == 1);

        // Step 5: Access a stateless service, which we authorized through
        // the call to makeUsersWithAuthSetupHelper() above.
        getManagementState(exampleWithMgmtAuthToken, true);

        // Step 6: Negative case, prove that the example user with only access to
        // example factory can not access the ServiceHostManagementService
        getManagementState(exampleAuthToken, false);

        this.host.log("AuthorizationSetupHelper is working");
    }

    @Test
    public void testRoleSetupWithLinks() throws Throwable {
        this.host.waitForServiceAvailable(ServiceHostManagementService.SELF_LINK);
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());

        AuthorizationSetupHelper.AuthSetupCompletion authCompletion = (ex) -> {
            if (ex == null) {
                this.host.completeIteration();
            } else {
                this.host.failIteration(ex);
            }
        };

        EnumSet<Action> verbs = EnumSet.of(Action.GET);

        this.host.testStart(1);
        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserSelfLink(ServiceUriPaths.CORE_AUTHZ_GUEST_USER)
                .setDocumentLink(ServiceUriPaths.SWAGGER)
                .setUserGroupName(GUEST_USER_GROUP)
                .setResourceGroupName(GUEST_RESOURCE_GROUP)
                .setRoleName(GUEST_ROLE)
                .setVerbs(verbs)
                .setCompletion(authCompletion)
                .setupRole();

        this.host.testWait();

        ServiceDocumentQueryResult result = queryDocuments(Utils.buildKind(UserGroupState.class),
                1);
        UserGroupState userGroupState = Utils
                .fromJson(result.documents.values().iterator().next(), UserGroupState.class);
        assertEquals(GUEST_USER_GROUP,
                UriUtils.getLastPathSegment(userGroupState.documentSelfLink));

        result = queryDocuments(Utils.buildKind(ResourceGroupState.class), 1);
        ResourceGroupState resourceGroupState = Utils
                .fromJson(result.documents.values().iterator().next(), ResourceGroupState.class);
        assertEquals(GUEST_RESOURCE_GROUP,
                UriUtils.getLastPathSegment(resourceGroupState.documentSelfLink));

        result = queryDocuments(Utils.buildKind(RoleState.class), 1);
        RoleState roleState = Utils
                .fromJson(result.documents.values().iterator().next(), RoleState.class);
        assertEquals(UriUtils.buildUriPath(UserGroupService.FACTORY_LINK, GUEST_USER_GROUP),
                roleState.userGroupLink);
        assertEquals(UriUtils.buildUriPath(ResourceGroupService.FACTORY_LINK, GUEST_RESOURCE_GROUP),
                roleState.resourceGroupLink);
        assertEquals(GUEST_ROLE, UriUtils.getLastPathSegment(roleState.documentSelfLink));
        assertEquals(roleState.verbs, verbs);
    }

    @Test
    public void testRoleSetupWithQueries() throws Throwable {
        this.host.waitForServiceAvailable(ServiceHostManagementService.SELF_LINK);
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());

        AuthorizationSetupHelper.AuthSetupCompletion authCompletion = (ex) -> {
            if (ex == null) {
                this.host.completeIteration();
            } else {
                this.host.failIteration(ex);
            }
        };

        this.host.testStart(1);

        Query userQuery = Query.Builder.create()
                .addFieldClause(
                        ServiceDocument.FIELD_NAME_SELF_LINK,
                        ServiceUriPaths.CORE_AUTHZ_GUEST_USER)
                .build();

        Query resourceQuery = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        ServiceUriPaths.SWAGGER, Occurance.SHOULD_OCCUR)
                .build();

        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserGroupQuery(userQuery)
                .setResourceQuery(resourceQuery)
                .setRoleName(GUEST_ROLE)
                .setCompletion(authCompletion)
                .setupRole();

        this.host.testWait();

        ServiceDocumentQueryResult result = queryDocuments(Utils.buildKind(UserGroupState.class),
                1);
        UserGroupState userGroupState = Utils
                .fromJson(result.documents.values().iterator().next(), UserGroupState.class);
        assertEquals(userQuery.booleanClauses.get(0).term.propertyName,
                userGroupState.query.booleanClauses.get(0).term.propertyName);
        assertEquals(userQuery.booleanClauses.get(0).term.matchValue,
                userGroupState.query.booleanClauses.get(0).term.matchValue);

        result = queryDocuments(Utils.buildKind(ResourceGroupState.class), 1);
        ResourceGroupState resourceGroupState = Utils
                .fromJson(result.documents.values().iterator().next(), ResourceGroupState.class);
        assertEquals(resourceQuery.booleanClauses.get(0).term.propertyName,
                resourceGroupState.query.booleanClauses.get(0).term.propertyName);
        assertEquals(resourceQuery.booleanClauses.get(0).term.matchValue,
                resourceGroupState.query.booleanClauses.get(0).term.matchValue);

        result = queryDocuments(Utils.buildKind(RoleState.class), 1);
        RoleState roleState = Utils
                .fromJson(result.documents.values().iterator().next(), RoleState.class);
        assertEquals(GUEST_ROLE, UriUtils.getLastPathSegment(roleState.documentSelfLink));
    }

    /**
     * Supports testAuthSetupHelper() by invoking the AuthorizationSetupHelper to
     * create two users with associated user groups, resource groups, and roles
     */
    private void makeUsersWithAuthSetupHelper() throws Throwable {
        AuthorizationSetupHelper.AuthSetupCompletion authCompletion = (ex) -> {
            if (ex == null) {
                this.host.completeIteration();
            } else {
                this.host.failIteration(ex);
            }
        };

        this.host.testStart(3);
        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserEmail(this.adminUser)
                .setUserPassword(this.adminUser)
                .setIsAdmin(true)
                .setCompletion(authCompletion)
                .start();

        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserEmail(this.exampleUser)
                .setUserPassword(this.exampleUser)
                .setIsAdmin(false)
                .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
                .setCompletion(authCompletion)
                .start();

        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserEmail(this.exampleWithManagementServiceUser)
                .setUserPassword(this.exampleWithManagementServiceUser)
                .setIsAdmin(false)
                .setDocumentLink(ServiceHostManagementService.SELF_LINK)
                .setCompletion(authCompletion)
                .start();

        this.host.testWait();
    }

    /**
     * Supports testAuthSetupHelper() by creating an example document. The document
     * is created with the auth token as returned by the login above, so it is
     * created with a specific user.
     */
    private void createExampleDocument(String authToken) throws Throwable {
        ExampleServiceState example = new ExampleServiceState();
        example.name = UUID.randomUUID().toString();
        URI exampleUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);

        Operation examplePost = Operation.createPost(exampleUri)
                .setBody(example)
                .forceRemote()
                .addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, authToken)
                .setCompletion(this.host.getCompletion());
        clearClientCookieJar();

        this.host.testStart(1);
        this.host.send(examplePost);
        this.host.testWait();
    }

    /**
     * Supports testAuthSetupHelper() by counting how many example documents we can
     * see with the given user (as indicated by the authToken)
     */
    private int numberExampleDocuments(String authToken) throws Throwable {
        URI exampleUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);

        Integer[] numberDocuments = new Integer[1];
        Operation get = Operation.createGet(exampleUri)
                .forceRemote()
                .addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, authToken)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                        return;
                    }
                    ServiceDocumentQueryResult response = op
                            .getBody(ServiceDocumentQueryResult.class);
                    assertTrue(response != null && response.documentLinks != null);
                    numberDocuments[0] = response.documentLinks.size();
                    this.host.completeIteration();
                });
        clearClientCookieJar();

        this.host.testStart(1);
        this.host.send(get);
        this.host.testWait();
        return numberDocuments[0];
    }

    private void getManagementState(String exampleWithMgmtAuthToken, boolean isAuthorized)
            throws Throwable {
        this.host.testStart(1);
        Operation get = Operation
                .createGet(UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK))
                .addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, exampleWithMgmtAuthToken)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        if (isAuthorized) {
                            this.host.failIteration(e);
                        } else {
                            this.host.completeIteration();
                        }
                        return;
                    }
                    ServiceHostState rsp = o.getBody(ServiceHostState.class);
                    if (rsp.httpPort != this.host.getPort()) {
                        this.host.failIteration(
                                new IllegalStateException("mgmt service state is not correct"));
                        return;
                    }
                    this.host.completeIteration();
                });

        this.host.send(get);
        this.host.testWait();
    }

    /**
     * Clear NettyHttpServiceClient's cookie jar
     *
     * The NettyHttpServiceClient is nice: it tracks what cookies we receive and sets them
     * on the outgoing connection. However, for some of our tests here, we want to control
     * the cookies to ensure they're not being used. Therefore we clear the cookie jar.
     *
     * Note that this shouldn't strictly be necessary: the server prefers the auth token
     * over the cookie. We're just being strict
     */
    private void clearClientCookieJar() {
        NettyHttpServiceClient client = (NettyHttpServiceClient) this.host.getClient();
        client.clearCookieJar();
    }

    private ServiceDocumentQueryResult queryDocuments(String documentKind, int desiredCount)
            throws Throwable {
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(documentKind);
        q.options = EnumSet
                .of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
        return this.host
                .createAndWaitSimpleDirectQuery(this.host.getUri(), q, desiredCount, desiredCount);
    }

    @Test
    public void testCompletionHandlerWhenUserExists() throws Throwable {
        this.host.waitForServiceAvailable(ServiceHostManagementService.SELF_LINK);
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());

        // create users
        makeUsersWithAuthSetupHelper();

        boolean[] isCalled = new boolean[1];

        TestContext testContext = this.host.testCreate(1);

        AuthorizationSetupHelper.AuthSetupCompletion authCompletion = (ex) -> {
            if (ex == null) {
                isCalled[0] = true;
                testContext.completeIteration();
            } else {
                testContext.failIteration(ex);
            }
        };

        // try to create existing user
        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserEmail(this.adminUser)
                .setUserPassword(this.adminUser)
                .setIsAdmin(true)
                .setCompletion(authCompletion)
                .start();

        testContext.await();

        assertTrue("completion handler must be called when trying to create an existing user",
                isCalled[0]);
    }

}
