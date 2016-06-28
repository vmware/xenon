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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationCacheUtils;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.GuestUserService;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.UserGroupService;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;
import com.vmware.xenon.services.common.UserService.UserState;

public class TestAuthorization extends BasicTestCase {

    public static class AuthzStatelessService extends StatelessService {
        public void handleRequest(Operation op) {
            if (op.getAction() == Action.PATCH) {
                op.complete();
                return;
            }
            super.handleRequest(op);
        }
    }

    private String userServicePath;
    private AuthorizationHelper authHelper;
    private int serviceCount = 10;

    @Override
    public void beforeHostStart(VerificationHost host) {
        // Enable authorization service; this is an end to end test
        host.setAuthorizationService(new AuthorizationContextService());
        host.setAuthorizationEnabled(true);
    }

    @Before
    public void enableTracing() throws Throwable {
        // Enable operation tracing to verify tracing does not error out with auth enabled.
        this.host.toggleOperationTracing(this.host.getUri(), true);
    }

    @After
    public void disableTracing() throws Throwable {
        this.host.toggleOperationTracing(this.host.getUri(), false);
    }

    @Before
    public void setupRoles() throws Throwable {
        this.host.setSystemAuthorizationContext();
        this.authHelper = new AuthorizationHelper(this.host);
        this.userServicePath = this.authHelper.createUserService(this.host, "jane@doe.com");
        this.authHelper.createRoles(this.host, "jane@doe.com");
        this.host.resetAuthorizationContext();
    }

    @Test
    public void statelessServiceAuthorization() throws Throwable {
        // assume system identity so we can create roles
        this.host.setSystemAuthorizationContext();

        String serviceLink = UUID.randomUUID().toString();

        // create a specific role for a stateless service
        String resourceGroupLink = this.authHelper.createResourceGroup(this.host,
                "stateless-service-group", Builder.create()
                        .addFieldClause(
                                ServiceDocument.FIELD_NAME_SELF_LINK,
                                UriUtils.URI_PATH_CHAR + serviceLink)
                        .build());
        this.authHelper.createRole(this.host, this.authHelper.getUserGroupLink(),
                resourceGroupLink,
                new HashSet<>(Arrays.asList(Action.GET, Action.POST, Action.PATCH, Action.DELETE)));
        this.host.resetAuthorizationContext();

        CompletionHandler ch = (o, e) -> {
            if (e == null || o.getStatusCode() != Operation.STATUS_CODE_FORBIDDEN) {
                this.host.failIteration(new IllegalStateException(
                        "Operation did not fail with proper status code"));
                return;
            }
            this.host.completeIteration();
        };

        // assume authorized user identity
        this.host.assumeIdentity(this.userServicePath);

        // Verify startService
        Operation post = Operation.createPost(UriUtils.buildUri(this.host, serviceLink));
        // do not supply a body, authorization should still be applied
        this.host.testStart(1);
        post.setCompletion(this.host.getCompletion());
        this.host.startService(post, new AuthzStatelessService());
        this.host.testWait();

        // stop service so we can attempt restart
        this.host.testStart(1);
        Operation delete = Operation.createDelete(post.getUri())
                .setCompletion(this.host.getCompletion());
        this.host.send(delete);
        this.host.testWait();

        // Verify DENY startService
        this.host.resetAuthorizationContext();
        this.host.testStart(1);
        post = Operation.createPost(UriUtils.buildUri(this.host, serviceLink));
        post.setCompletion(ch);
        this.host.startService(post, new AuthzStatelessService());
        this.host.testWait();

        // assume authorized user identity
        this.host.assumeIdentity(this.userServicePath);

        // restart service
        post = Operation.createPost(UriUtils.buildUri(this.host, serviceLink));
        // do not supply a body, authorization should still be applied
        this.host.testStart(1);
        post.setCompletion(this.host.getCompletion());
        this.host.startService(post, new AuthzStatelessService());
        this.host.testWait();

        // Verify PATCH
        Operation patch = Operation.createPatch(UriUtils.buildUri(this.host, serviceLink));
        patch.setBody(new ServiceDocument());
        this.host.testStart(1);
        patch.setCompletion(this.host.getCompletion());
        this.host.send(patch);
        this.host.testWait();

        // Verify DENY PATCH
        this.host.resetAuthorizationContext();
        patch = Operation.createPatch(UriUtils.buildUri(this.host, serviceLink));
        patch.setBody(new ServiceDocument());
        this.host.testStart(1);
        patch.setCompletion(ch);
        this.host.send(patch);
        this.host.testWait();
    }

    @Test
    public void queryWithDocumentAuthPrincipal() throws Throwable {
        this.host.assumeIdentity(this.userServicePath);
        createExampleServices("jane");

        // do a direct, simple query first
        this.host.createAndWaitSimpleDirectQuery(ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK,
                this.userServicePath, this.serviceCount, this.serviceCount);

        // now do a paginated query to verify we can get to paged results with authz enabled
        QueryTask qt = QueryTask.Builder.create().setResultLimit(this.serviceCount / 2)
                .build();
        qt.querySpec.query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK,
                        this.userServicePath)
                .build();

        URI taskUri = this.host.createQueryTaskService(qt);
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {

            qt = this.host.getServiceState(null, QueryTask.class, taskUri);
            if (TaskState.isFailed(qt.taskInfo)) {
                throw new IllegalStateException("task failed");
            }
            if (TaskState.isFinished(qt.taskInfo)) {
                break;
            }
            this.host.log("Task not finished");
            Thread.sleep(100);
        }

        if (new Date().after(exp)) {
            throw new TimeoutException();
        }

        this.host.testStart(1);
        Operation get = Operation.createGet(UriUtils.buildUri(this.host, qt.results.nextPageLink))
                .setCompletion(this.host.getCompletion());
        this.host.send(get);
        this.host.testWait();
    }

    @Test
    public void contextPropagationOnScheduleAndRunContext() throws Throwable {
        this.host.assumeIdentity(this.userServicePath);

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
    public void guestAuthorization() throws Throwable {
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());

        // Create user group for guest user
        String userGroupLink =
                this.authHelper.createUserGroup(this.host, "guest-user-group", Builder.create()
                        .addFieldClause(
                                ServiceDocument.FIELD_NAME_SELF_LINK,
                                GuestUserService.SELF_LINK)
                        .build());
        // Create resource group for example service state
        String exampleServiceResourceGroupLink =
                this.authHelper.createResourceGroup(this.host, "guest-resource-group", Builder.create()
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_KIND,
                                Utils.buildKind(ExampleServiceState.class))
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_NAME,
                                "guest")
                        .build());

        // Create roles tying these together
        this.authHelper.createRole(this.host, userGroupLink, exampleServiceResourceGroupLink,
                new HashSet<>(Arrays.asList(Action.GET, Action.POST)));

        // Create some example services; some accessible, some not
        Map<URI, ExampleServiceState> exampleServices = new HashMap<>();
        exampleServices.putAll(createExampleServices("jane"));
        exampleServices.putAll(createExampleServices("guest"));

        OperationContext.setAuthorizationContext(null);

        // Execute get on factory trying to get all example services
        final ServiceDocumentQueryResult[] factoryGetResult = new ServiceDocumentQueryResult[1];
        Operation getFactory = Operation.createGet(
                UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
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
        assertAuthorizedServicesInResult("guest", exampleServices, factoryGetResult[0]);

    }

    @Test
    public void actionBasedAuthorization() throws Throwable {

        // Assume Jane's identity
        this.host.assumeIdentity(this.userServicePath);

        // add docs accessible by jane
        Map<URI, ExampleServiceState> exampleServices = createExampleServices("jane");

        // Execute get on factory trying to get all example services
        final ServiceDocumentQueryResult[] factoryGetResult = new ServiceDocumentQueryResult[1];
        Operation getFactory = Operation.createGet(
                UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
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

        // DELETE operation should be denied
        Set<String> selfLinks = new HashSet<>(factoryGetResult[0].documentLinks);
        for (String selfLink : selfLinks) {
            Operation deleteOperation =
                    Operation.createDelete(UriUtils.buildUri(this.host, selfLink))
                            .setCompletion((o, e) -> {
                                if (o.getStatusCode() != Operation.STATUS_CODE_FORBIDDEN) {
                                    String message = String.format("Expected %d, got %s",
                                            Operation.STATUS_CODE_FORBIDDEN,
                                            o.getStatusCode());
                                    this.host.failIteration(new IllegalStateException(message));
                                    return;
                                }

                                this.host.completeIteration();
                            });
            this.host.testStart(1);
            this.host.send(deleteOperation);
            this.host.testWait();
        }

        // PATCH operation should be allowed
        for (String selfLink : selfLinks) {
            Operation patchOperation =
                    Operation.createPatch(UriUtils.buildUri(this.host, selfLink))
                        .setBody(exampleServices.get(selfLink))
                        .setCompletion((o, e) -> {
                            if (o.getStatusCode() != Operation.STATUS_CODE_OK) {
                                String message = String.format("Expected %d, got %s",
                                        Operation.STATUS_CODE_OK,
                                        o.getStatusCode());
                                this.host.failIteration(new IllegalStateException(message));
                                return;
                            }

                            this.host.completeIteration();
                        });
            this.host.testStart(1);
            this.host.send(patchOperation);
            this.host.testWait();
        }
    }

    @Test
    public void statefulServiceAuthorization() throws Throwable {
        // Create example services not accessible by jane (as the system user)
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        Map<URI, ExampleServiceState> exampleServices = createExampleServices("john");

        // try to create services with no user context set; we should get a 403
        OperationContext.setAuthorizationContext(null);
        ExampleServiceState state = createExampleServiceState("jane", new Long("100"));
        this.host.testStart(1);
        this.host.send(
                Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
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
                Operation.createGet(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
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
        this.host.assumeIdentity(this.userServicePath);
        // add docs accessible by jane
        exampleServices.putAll(createExampleServices("jane"));

        verifyJaneAccess(exampleServices, null);

        // Execute get on factory trying to get all example services
        final ServiceDocumentQueryResult[] factoryGetResult = new ServiceDocumentQueryResult[1];
        Operation getFactory = Operation.createGet(
                UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
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
        assertAuthorizedServicesInResult("jane", exampleServices, factoryGetResult[0]);

        // Execute query task trying to get all example services
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(Utils.buildKind(ExampleServiceState.class));
        URI u = this.host.createQueryTaskService(QueryTask.create(q));
        QueryTask task = this.host.waitForQueryTaskCompletion(q, 1, 1, u, false, true, false);
        assertEquals(TaskState.TaskStage.FINISHED, task.taskInfo.stage);

        // Make sure only the authorized services were returned
        assertAuthorizedServicesInResult("jane", exampleServices, task.results);

        // reset the auth context
        OperationContext.setAuthorizationContext(null);

        // Assume Jane's identity through header auth token
        String authToken = generateAuthToken(this.userServicePath);

        verifyJaneAccess(exampleServices, authToken);
    }

    private AuthorizationContext assumeIdentityAndGetContext(String userLink,
            Service privilegedService, boolean populateCache) throws Throwable {
        AuthorizationContext authContext = this.host.assumeIdentity(userLink);
        if (populateCache) {
            this.host.sendAndWaitExpectSuccess(
                    Operation.createGet(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK)));
        }
        return this.host.getAuthorizationContext(privilegedService, authContext.getToken());
    }

    @Test
    public void testAuthzUtils() throws Throwable {
        this.host.setSystemAuthorizationContext();
        AuthorizationHelper authHelperForFoo = new AuthorizationHelper(this.host);
        String email = "foo@foo.com";
        String fooUserLink = authHelperForFoo.createUserService(this.host, email);
        UserState patchState = new UserState();
        patchState.userGroupLinks = new HashSet<String>();
        patchState.userGroupLinks.add(UriUtils.buildUriPath(
                UserGroupService.FACTORY_LINK, authHelperForFoo.getUserGroupName(email)));
        authHelperForFoo.patchUserService(this.host, fooUserLink, patchState);
        // create a user group based on a query for userGroupLink
        authHelperForFoo.createRoles(this.host, email, true);
        // spin up a privileged service to query for auth context
        MinimalTestService s = new MinimalTestService();
        this.host.addPrivilegedService(MinimalTestService.class);
        this.host.startServiceAndWait(s, UUID.randomUUID().toString(), null);
        this.host.resetSystemAuthorizationContext();

        // get the user group service and clear the authz cache
        assertNotNull(assumeIdentityAndGetContext(fooUserLink, s, true));
        this.host.setSystemAuthorizationContext();
        Operation getUserGroupStateOp =
                Operation.createGet(UriUtils.buildUri(this.host, authHelperForFoo.getUserGroupLink()));
        Operation resultOp = this.host.waitForResponse(getUserGroupStateOp);
        UserGroupState userGroupState = resultOp.getBody(UserGroupState.class);
        Operation clearAuthOp = new Operation();
        TestContext ctx = this.host.testCreate(1);
        clearAuthOp.setCompletion(ctx.getCompletion());
        AuthorizationCacheUtils.clearAuthzCacheForUserGroup(s, clearAuthOp, userGroupState);
        clearAuthOp.complete();
        this.host.testWait(ctx);
        this.host.resetSystemAuthorizationContext();
        assertNull(assumeIdentityAndGetContext(fooUserLink, s, false));

        // get the resource group and clear the authz cache
        assertNotNull(assumeIdentityAndGetContext(fooUserLink, s, true));
        this.host.setSystemAuthorizationContext();
        Operation getResourceGroupStateOp =
                Operation.createGet(UriUtils.buildUri(this.host, authHelperForFoo.getResourceGroupLink()));
        resultOp = this.host.waitForResponse(getResourceGroupStateOp);
        ResourceGroupState resourceGroupState = resultOp.getBody(ResourceGroupState.class);
        clearAuthOp = new Operation();
        ctx = this.host.testCreate(1);
        clearAuthOp.setCompletion(ctx.getCompletion());
        AuthorizationCacheUtils.clearAuthzCacheForResourceGroup(s, clearAuthOp, resourceGroupState);
        clearAuthOp.complete();
        this.host.testWait(ctx);
        this.host.resetSystemAuthorizationContext();
        assertNull(assumeIdentityAndGetContext(fooUserLink, s, false));

        // get the role service and clear the authz cache
        assertNotNull(assumeIdentityAndGetContext(fooUserLink, s, true));
        this.host.setSystemAuthorizationContext();
        Operation getRoleStateOp =
                Operation.createGet(UriUtils.buildUri(this.host, authHelperForFoo.getRoleLink()));
        resultOp = this.host.waitForResponse(getRoleStateOp);
        RoleState roleState = resultOp.getBody(RoleState.class);
        clearAuthOp = new Operation();
        ctx = this.host.testCreate(1);
        clearAuthOp.setCompletion(ctx.getCompletion());
        AuthorizationCacheUtils.clearAuthzCacheForRole(s, clearAuthOp, roleState);
        clearAuthOp.complete();
        this.host.testWait(ctx);
        this.host.resetSystemAuthorizationContext();
        assertNull(assumeIdentityAndGetContext(fooUserLink, s, false));

        // finally, get the user service and clear the authz cache
        assertNotNull(assumeIdentityAndGetContext(fooUserLink, s, true));
        this.host.setSystemAuthorizationContext();
        Operation getUserStateOp =
                Operation.createGet(UriUtils.buildUri(this.host, fooUserLink));
        resultOp = this.host.waitForResponse(getUserStateOp);
        UserState userState = resultOp.getBody(UserState.class);
        clearAuthOp = new Operation();
        ctx = this.host.testCreate(1);
        clearAuthOp.setCompletion(ctx.getCompletion());
        AuthorizationCacheUtils.clearAuthzCacheForUser(s, clearAuthOp, userState.documentSelfLink);
        clearAuthOp.complete();
        this.host.testWait(ctx);
        this.host.resetSystemAuthorizationContext();
        assertNull(assumeIdentityAndGetContext(fooUserLink, s, false));
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

    private void assertAuthorizedServicesInResult(String name,
            Map<URI, ExampleServiceState> exampleServices,
            ServiceDocumentQueryResult result) {
        Set<String> selfLinks = new HashSet<>(result.documentLinks);
        for (Entry<URI, ExampleServiceState> entry : exampleServices.entrySet()) {
            String selfLink = entry.getKey().getPath();
            if (entry.getValue().name.equals(name)) {
                assertTrue(selfLinks.contains(selfLink));
            } else {
                assertFalse(selfLinks.contains(selfLink));
            }
        }
    }

    private String generateAuthToken(String userServicePath) throws GeneralSecurityException {
        Claims.Builder builder = new Claims.Builder();
        builder.setSubject(userServicePath);
        Claims claims = builder.getResult();
        return this.host.getTokenSigner().sign(claims);
    }

    private ExampleServiceState createExampleServiceState(String name, Long counter) {
        ExampleServiceState state = new ExampleServiceState();
        state.name = name;
        state.counter = counter;
        state.documentAuthPrincipalLink = "stringtooverwrite";
        return state;
    }

    private Map<URI, ExampleServiceState> createExampleServices(String userName) throws Throwable {
        Collection<ExampleServiceState> bodies = new LinkedList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            bodies.add(createExampleServiceState(userName, 1L));
        }

        Iterator<ExampleServiceState> it = bodies.iterator();
        Consumer<Operation> bodySetter = (o) -> {
            o.setBody(it.next());
        };

        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(
                null,
                bodies.size(),
                ExampleServiceState.class,
                bodySetter,
                UriUtils.buildFactoryUri(this.host, ExampleService.class));

        return states;
    }
}
