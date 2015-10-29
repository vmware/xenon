/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.Operation.AuthorizationContext;
import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.test.VerificationHost;
import com.vmware.dcp.services.common.AuthorizationContextService;
import com.vmware.dcp.services.common.ExampleFactoryService;
import com.vmware.dcp.services.common.ExampleService.ExampleServiceState;
import com.vmware.dcp.services.common.QueryTask;
import com.vmware.dcp.services.common.QueryTask.Query;
import com.vmware.dcp.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.dcp.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.dcp.services.common.RoleService.Policy;
import com.vmware.dcp.services.common.RoleService.RoleState;
import com.vmware.dcp.services.common.ServiceUriPaths;
import com.vmware.dcp.services.common.UserGroupService.UserGroupState;
import com.vmware.dcp.services.common.UserService.UserState;

public class TestAuthorization extends BasicTestCase {

    String userServicePath;

    @Override
    public void beforeHostStart(VerificationHost host) {
        // Enable authorization service; this is an end to end test
        host.setAuthorizationService(new AuthorizationContextService());
        host.setAuthorizationEnabled(true);
    }

    @Before
    public void setupRoles() throws Throwable {
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        this.userServicePath = createUser("jane@doe.com");
        createRoles();
        OperationContext.setAuthorizationContext(null);
    }

    @Test
    public void testAuthPrincipalQuery() throws Throwable {
        assumeIdentity(this.userServicePath);
        createExampleServices("jane");
        host.createAndWaitSimpleDirectQuery(ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK,
                this.userServicePath, 2, 2);
    }

    @Test
    public void testScheduleAndRunContext() throws Throwable {
        assumeIdentity(this.userServicePath);

        AuthorizationContext callerAuthContext = OperationContext.getAuthorizationContext();
        Runnable task = () -> {
            if (OperationContext.getAuthorizationContext().equals(callerAuthContext)) {
                this.host.completeIteration();
                return;
            }
            this.host.failIteration(new IllegalStateException("Incorrect auth context obtained"));
        };

        this.host.testStart(1);
        this.host.schedule(task, 1, TimeUnit.MILLISECONDS);
        this.host.testWait();

        this.host.testStart(1);
        this.host.run(task);
        this.host.testWait();
    }

    @Test
    public void exampleAuthorization() throws Throwable {
        // Create example services not accessible by jane (as the system user)
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        Map<URI, ExampleServiceState> exampleServices = createExampleServices("john");

        // try to create services with no user context set; we should get a 403
        OperationContext.setAuthorizationContext(null);
        ExampleServiceState state = exampleServiceState("jane", new Long("100"));
        this.host.testStart(1);
        this.host.send(
                Operation.createPost(UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK))
                        .setBody(state)
                        .setCompletion((o, e) -> {
                            if (o.getStatusCode() != Operation.STATUS_CODE_FORBIDDEN) {
                                String message = String.format("Expected %d, got %s",
                                        Operation.STATUS_CODE_FORBIDDEN,
                                        o.getStatusCode());
                                this.host.failIteration(new IllegalStateException(message));
                                return;
                            }

                            this.host.completeIteration();
                        }));
        this.host.testWait();

        // issue a GET on a factory with no auth context, no documents should be returned
        this.host.testStart(1);
        this.host.send(
                Operation.createGet(UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(new IllegalStateException(e));
                        return;
                    }
                    ServiceDocumentQueryResult res = o
                            .getBody(ServiceDocumentQueryResult.class);
                    if (!res.documentLinks.isEmpty()) {
                        String message = String.format("Expected 0 results; Got %d",
                                res.documentLinks.size());
                        this.host.failIteration(new IllegalStateException(message));
                        return;
                    }

                    this.host.completeIteration();
                }));
        this.host.testWait();

        // Assume Jane's identity
        assumeIdentity(this.userServicePath);
        // add docs accessible by jane
        exampleServices.putAll(createExampleServices("jane"));

        verifyJaneAccess(exampleServices, null);

        // Execute get on factory trying to get all example services
        final ServiceDocumentQueryResult[] factoryGetResult = new ServiceDocumentQueryResult[1];
        Operation getFactory = Operation.createGet(UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    factoryGetResult[0] = o.getBody(ServiceDocumentQueryResult.class);
                    this.host.completeIteration();
                });

        this.host.testStart(1);
        this.host.send(getFactory);
        this.host.testWait();

        // Make sure only the authorized services were returned
        assertAuthorizedServicesInResult(exampleServices, factoryGetResult[0]);

        // Execute query task trying to get all example services
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        URI u = this.host.createQueryTaskService(QueryTask.create(q));
        QueryTask task = this.host.waitForQueryTaskCompletion(q, 1, 1, u, false, true, false);
        assertEquals(TaskState.TaskStage.FINISHED, task.taskInfo.stage);

        // Make sure only the authorized services were returned
        assertAuthorizedServicesInResult(exampleServices, task.results);

        // reset the auth context
        OperationContext.setAuthorizationContext(null);

        // Assume Jane's identity through header auth token
        String authToken = generateAuthToken(this.userServicePath);

        verifyJaneAccess(exampleServices, authToken);
    }

    private void verifyJaneAccess(Map<URI, ExampleServiceState> exampleServices, String authToken) throws Throwable {
        // Try to GET all example services
        this.host.testStart(exampleServices.size());
        for (Entry<URI, ExampleServiceState> entry : exampleServices.entrySet()) {
            Operation get = Operation.createGet(entry.getKey());
            // force to create a remote context
            if (authToken != null) {
                get.forceRemote();
                get.getRequestHeaders().put(Operation.REQUEST_AUTH_TOKEN_HEADER, authToken);
            }
            if (entry.getValue().name.equals("jane")) {
                // Expect 200 OK
                get.setCompletion((o, e) -> {
                    if (o.getStatusCode() != Operation.STATUS_CODE_OK) {
                        String message = String.format("Expected %d, got %s",
                                Operation.STATUS_CODE_OK,
                                o.getStatusCode());
                        this.host.failIteration(new IllegalStateException(message));
                        return;
                    }
                    ExampleServiceState body = o.getBody(ExampleServiceState.class);
                    if (!body.documentAuthPrincipalLink.equals(this.userServicePath)) {
                        String message = String.format("Expected %s, got %s",
                                this.userServicePath, body.documentAuthPrincipalLink);
                        this.host.failIteration(new IllegalStateException(message));
                        return;
                    }
                    this.host.completeIteration();
                });
            } else {
                // Expect 403 Forbidden
                get.setCompletion((o, e) -> {
                    if (o.getStatusCode() != Operation.STATUS_CODE_FORBIDDEN) {
                        String message = String.format("Expected %d, got %s",
                                Operation.STATUS_CODE_FORBIDDEN,
                                o.getStatusCode());
                        this.host.failIteration(new IllegalStateException(message));
                        return;
                    }

                    this.host.completeIteration();
                });
            }

            this.host.send(get);
        }
        this.host.testWait();
    }

    private void assertAuthorizedServicesInResult(Map<URI, ExampleServiceState> exampleServices,
            ServiceDocumentQueryResult result) {
        Set<String> selfLinks = new HashSet<>(result.documentLinks);
        for (Entry<URI, ExampleServiceState> entry : exampleServices.entrySet()) {
            String selfLink = entry.getKey().getPath();
            if (entry.getValue().name.equals("jane")) {
                assertTrue(selfLinks.contains(selfLink));
            } else {
                assertFalse(selfLinks.contains(selfLink));
            }
        }
    }

    private void createRoles() throws Throwable {
        this.host.testStart(3);

        URI postUserGroupsUri =
                UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);

        URI postResourceGroupsUri =
                UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);

        // Create user group
        UserGroupState userGroupState = new UserGroupState();
        userGroupState.documentSelfLink = "janes-user-group";
        userGroupState.query = new Query();
        userGroupState.query.setTermPropertyName("email");
        userGroupState.query.setTermMatchType(MatchType.TERM);
        userGroupState.query.setTermMatchValue("jane@doe.com");

        this.host.send(Operation
                .createPost(postUserGroupsUri)
                .setBody(userGroupState)
                .setCompletion(this.host.getCompletion()));

        // Create resource group for example service state
        ResourceGroupState exampleResourceGroupState = new ResourceGroupState();
        exampleResourceGroupState.documentSelfLink = "janes-resource-group";
        exampleResourceGroupState.query = new Query();

        Query kindClause = new Query();
        kindClause.setTermPropertyName(ExampleServiceState.FIELD_NAME_KIND);
        kindClause.setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        kindClause.setTermMatchType(MatchType.TERM);
        exampleResourceGroupState.query.addBooleanClause(kindClause);

        Query nameClause = new Query();
        nameClause.setTermPropertyName(ExampleServiceState.FIELD_NAME_NAME);
        nameClause.setTermMatchValue("jane");
        nameClause.setTermMatchType(MatchType.TERM);
        exampleResourceGroupState.query.addBooleanClause(nameClause);

        this.host.send(Operation
                .createPost(postResourceGroupsUri)
                .setBody(exampleResourceGroupState)
                .setCompletion(this.host.getCompletion()));

        // Create resource group to allow GETs on ALL query tasks
        ResourceGroupState queryTaskResourceGroupState = new ResourceGroupState();
        queryTaskResourceGroupState.documentSelfLink = "any-query-task-resource-group";
        queryTaskResourceGroupState.query = new Query();

        Query queryTaskKindClause = new Query();
        queryTaskKindClause.setTermPropertyName(ExampleServiceState.FIELD_NAME_KIND);
        queryTaskKindClause.setTermMatchValue(Utils.buildKind(QueryTask.class));
        queryTaskKindClause.setTermMatchType(MatchType.TERM);
        queryTaskResourceGroupState.query.addBooleanClause(queryTaskKindClause);

        this.host.send(Operation
                .createPost(postResourceGroupsUri)
                .setBody(queryTaskResourceGroupState)
                .setCompletion(this.host.getCompletion()));

        this.host.testWait();

        // Create roles tying these together
        this.host.testStart(2);

        String userGroupLink = UriUtils
                .extendUri(postUserGroupsUri, userGroupState.documentSelfLink)
                .getPath();

        String exampleServiceResourceGroupLink = UriUtils
                .extendUri(postResourceGroupsUri, exampleResourceGroupState.documentSelfLink)
                .getPath();
        createRole(userGroupLink, exampleServiceResourceGroupLink);

        String queryTaskResourceGroupLink = UriUtils
                .extendUri(postResourceGroupsUri, queryTaskResourceGroupState.documentSelfLink)
                .getPath();
        createRole(userGroupLink, queryTaskResourceGroupLink);

        this.host.testWait();
    }

    private void createRole(String userGroupLink, String resourceGroupLink) {
        RoleState roleState = new RoleState();
        roleState.userGroupLink = userGroupLink;
        roleState.resourceGroupLink = resourceGroupLink;
        roleState.verbs = new HashSet<>();
        roleState.verbs.add(Action.GET);
        roleState.verbs.add(Action.POST);
        roleState.policy = Policy.ALLOW;

        this.host.send(Operation
                .createPost(UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_ROLES))
                .setBody(roleState)
                .setCompletion(this.host.getCompletion()));
    }

    private String createUser(String email) throws Throwable {
        final String[] userUriPath = new String[1];

        UserState userState = new UserState();
        userState.email = email;

        URI postUserUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USERS);
        this.host.testStart(1);
        this.host.send(Operation
                .createPost(postUserUri)
                .setBody(userState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    UserState state = o.getBody(UserState.class);
                    userUriPath[0] = state.documentSelfLink;
                    this.host.completeIteration();
                }));
        this.host.testWait();

        return userUriPath[0];
    }

    private void assumeIdentity(String userServicePath) throws GeneralSecurityException {
        Claims.Builder builder = new Claims.Builder();
        builder.setSubject(userServicePath);
        Claims claims = builder.getResult();
        String token = this.host.getTokenSigner().sign(claims);

        AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
        ab.setClaims(claims);
        ab.setToken(token);

        // Associate resulting authorization context with this thread
        OperationContext.setAuthorizationContext(ab.getResult());
    }

    private String generateAuthToken(String userServicePath) throws GeneralSecurityException {
        Claims.Builder builder = new Claims.Builder();
        builder.setSubject(userServicePath);
        Claims claims = builder.getResult();
        return this.host.getTokenSigner().sign(claims);
    }

    private ExampleServiceState exampleServiceState(String name, Long counter) {
        ExampleServiceState state = new ExampleServiceState();
        state.name = name;
        state.counter = counter;
        state.documentAuthPrincipalLink = "stringtooverwrite";
        return state;
    }

    private Map<URI, ExampleServiceState> createExampleServices(String userName) throws Throwable {
        Collection<ExampleServiceState> bodies = new LinkedList<>();
        bodies.add(exampleServiceState(userName, new Long(1)));
        bodies.add(exampleServiceState(userName, new Long(2)));

        Iterator<ExampleServiceState> it = bodies.iterator();
        Consumer<Operation> bodySetter = (o) -> {
            o.setBody(it.next());
        };

        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(
                null,
                bodies.size(),
                ExampleServiceState.class,
                bodySetter,
                UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK));

        return states;
    }
}
