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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.TestAuthorization.AuthzStatefulService.AuthzState;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.QueryTestUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationCacheUtils;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.GuestUserService;
import com.vmware.xenon.services.common.MinimalFactoryTestService;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.RoleService;
import com.vmware.xenon.services.common.RoleService.Policy;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.SystemUserService;
import com.vmware.xenon.services.common.TransactionService.TransactionServiceState;
import com.vmware.xenon.services.common.UserGroupService;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;
import com.vmware.xenon.services.common.UserService.UserState;

public class TestAuthorization extends BasicTestCase {

    public static class AuthzStatelessService extends StatelessService {
        @Override
        public void handleRequest(Operation op) {
            if (op.getAction() == Action.PATCH) {
                op.complete();
                return;
            }
            super.handleRequest(op);
        }
    }

    public static class AuthzStatefulService extends StatefulService {

        public static class AuthzState extends ServiceDocument {
            public String userLink;
        }

        public AuthzStatefulService() {
            super(AuthzState.class);
        }

        @Override
        public void handleStart(Operation post) {
            AuthzState body = post.getBody(AuthzState.class);
            AuthorizationContext authorizationContext = getAuthorizationContextForSubject(
                    body.userLink);

            if (authorizationContext == null ||
                    !authorizationContext.getClaims().getSubject().equals(body.userLink)) {
                post.fail(Operation.STATUS_CODE_INTERNAL_ERROR);
                return;
            }
            post.complete();
        }
    }

    public int serviceCount = 10;

    private String userServicePath;
    private AuthorizationHelper authHelper;

    @Override
    public void beforeHostStart(VerificationHost host) {
        // Enable authorization service; this is an end to end test
        host.setAuthorizationService(new AuthorizationContextService());
        host.setAuthorizationEnabled(true);
        CommandLineArgumentParser.parseFromProperties(this);
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
    public void factoryGetWithOData() {
        // GET with ODATA will be implicitly converted to a query task. Query tasks
        // require explicit authorization for the principal to be able to create them
        URI exampleFactoryUriWithOData = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK,
                "$limit=10");
        TestRequestSender sender = this.host.getTestRequestSender();
        FailureResponse rsp = sender.sendAndWaitFailure(Operation.createGet(exampleFactoryUriWithOData));
        ServiceErrorResponse errorRsp = rsp.op.getErrorResponseBody();
        assertTrue(errorRsp.message.toLowerCase().contains("forbidden"));
        assertTrue(errorRsp.message.contains(UriUtils.URI_PARAM_ODATA_TENANTLINKS));

        exampleFactoryUriWithOData = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK,
                "$filter=name eq someone");
        rsp = sender.sendAndWaitFailure(Operation.createGet(exampleFactoryUriWithOData));
        errorRsp = rsp.op.getErrorResponseBody();
        assertTrue(errorRsp.message.toLowerCase().contains("forbidden"));
        assertTrue(errorRsp.message.contains(UriUtils.URI_PARAM_ODATA_TENANTLINKS));

        // GET without ODATA should succeed but return empty result set
        URI exampleFactoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        Operation rspOp = sender.sendAndWait(Operation.createGet(exampleFactoryUri));
        ServiceDocumentQueryResult queryRsp = rspOp.getBody(ServiceDocumentQueryResult.class);
        assertEquals(0L, (long) queryRsp.documentCount);
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

        this.host.setOperationTracingLevel(Level.FINER);
        // Verify PATCH
        Operation patch = Operation.createPatch(UriUtils.buildUri(this.host, serviceLink));
        patch.setBody(new ServiceDocument());
        this.host.testStart(1);
        patch.setCompletion(this.host.getCompletion());
        this.host.send(patch);
        this.host.testWait();
        this.host.setOperationTracingLevel(Level.ALL);

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
    public void queryTasksDirectAndContinuous() throws Throwable {
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
        this.host.waitFor("task not finished in time", () -> {
            QueryTask r = this.host.getServiceState(null, QueryTask.class, taskUri);
            if (TaskState.isFailed(r.taskInfo)) {
                throw new IllegalStateException("task failed");
            }
            if (TaskState.isFinished(r.taskInfo)) {
                qt.taskInfo = r.taskInfo;
                qt.results = r.results;
                return true;
            }
            return false;
        });


        TestContext ctx = this.host.testCreate(1);
        Operation get = Operation.createGet(UriUtils.buildUri(this.host, qt.results.nextPageLink))
                .setCompletion(ctx.getCompletion());
        this.host.send(get);
        ctx.await();

        TestContext kryoCtx = this.host.testCreate(1);
        Operation patchOp = Operation.createPatch(this.host, ExampleService.FACTORY_LINK + "/foo")
                .setBody(new ServiceDocument())
                .setContentType(Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM)
                .setCompletion((o, e) -> {
                    if (e != null && o.getStatusCode() == Operation.STATUS_CODE_UNAUTHORIZED) {
                        kryoCtx.completeIteration();
                        return;
                    }
                    kryoCtx.failIteration(new IllegalStateException("expected a failure"));
                });
        this.host.send(patchOp);
        kryoCtx.await();

        int requestCount = this.serviceCount;
        TestContext notifyCtx = this.testCreate(requestCount);

        // Verify that even though updates to the index are performed
        // as a system user; the notification received by the subscriber of
        // the continuous query has the same authorization context as that of
        // user that created the continuous query.
        Consumer<Operation> notify = (o) -> {
            o.complete();
            String subject = o.getAuthorizationContext().getClaims().getSubject();
            if (!this.userServicePath.equals(subject)) {
                notifyCtx.fail(new IllegalStateException(
                        "Invalid auth subject in notification: " + subject));
                return;
            }
            this.host.log("Received authorized notification for index patch: %s", o.toString());
            notifyCtx.complete();
        };

        Query q = Query.Builder.create()
                .addKindFieldClause(ExampleServiceState.class)
                .build();
        QueryTask cqt = QueryTask.Builder.create().setQuery(q).build();

        // Create and subscribe to the continous query as an ordinary user.
        // do a continuous query, verify we receive some notifications
        URI notifyURI = QueryTestUtils.startAndSubscribeToContinuousQuery(
                this.host.getTestRequestSender(), this.host, cqt,
                notify);

        // issue updates, create some services as the system user
        this.host.setSystemAuthorizationContext();
        createExampleServices("jane");
        this.host.log("Waiting on continiuous query task notifications (%d)", requestCount);
        notifyCtx.await();
        this.host.resetSystemAuthorizationContext();

        this.host.assumeIdentity(this.userServicePath);
        QueryTestUtils.stopContinuousQuerySubscription(
                this.host.getTestRequestSender(), this.host, notifyURI,
                cqt);

    }

    @Test
    public void validateKryoOctetStreamRequests() throws Throwable {
        Consumer<Boolean> validate = (expectUnauthorizedResponse) -> {
            TestContext kryoCtx = this.host.testCreate(1);
            Operation patchOp = Operation.createPatch(this.host, ExampleService.FACTORY_LINK + "/foo")
                    .setBody(new ServiceDocument())
                    .setContentType(Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM)
                    .setCompletion((o, e) -> {
                        boolean isUnauthorizedResponse = o.getStatusCode() == Operation.STATUS_CODE_UNAUTHORIZED;
                        if (expectUnauthorizedResponse == isUnauthorizedResponse) {
                            kryoCtx.completeIteration();
                            return;
                        }
                        kryoCtx.failIteration(new IllegalStateException("Response did not match expectation"));
                    });
            this.host.send(patchOp);
            kryoCtx.await();
        };

        // Validate GUEST users are not authorized for sending kryo-octet-stream requests.
        this.host.resetAuthorizationContext();
        validate.accept(true);

        // Validate non-Guest, non-System users are also not authorized.
        this.host.assumeIdentity(this.userServicePath);
        validate.accept(true);

        // Validate System users are allowed.
        this.host.assumeIdentity(SystemUserService.SELF_LINK);
        validate.accept(false);
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
                new HashSet<>(Arrays.asList(Action.GET, Action.POST, Action.PATCH)));

        // Create some example services; some accessible, some not
        Map<URI, ExampleServiceState> exampleServices = new HashMap<>();
        exampleServices.putAll(createExampleServices("jane"));
        exampleServices.putAll(createExampleServices("guest"));

        OperationContext.setAuthorizationContext(null);

        TestRequestSender sender = this.host.getTestRequestSender();
        Operation responseOp = sender.sendAndWait(Operation.createGet(this.host, ExampleService.FACTORY_LINK));

        // Make sure only the authorized services were returned
        ServiceDocumentQueryResult getResult = responseOp.getBody(ServiceDocumentQueryResult.class);
        assertAuthorizedServicesInResult("guest", exampleServices, getResult);
        String guestLink = getResult.documentLinks.iterator().next();

        // Make sure we are able to PATCH the example service.
        ExampleServiceState state = new ExampleServiceState();
        state.counter = 2L;
        responseOp = sender.sendAndWait(Operation.createPatch(this.host, guestLink).setBody(state));
        assertEquals(Operation.STATUS_CODE_OK, responseOp.getStatusCode());

        // Let's try to do another PATCH using kryo-octet-stream
        state.counter = 3L;
        FailureResponse failureResponse = sender.sendAndWaitFailure(
                Operation.createPatch(this.host, guestLink)
                        .setContentType(Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM)
                        .setBody(state));
        assertEquals(Operation.STATUS_CODE_UNAUTHORIZED, failureResponse.op.getStatusCode());

        Map<String, ServiceStats.ServiceStat> stat = this.host.getServiceStats(
                UriUtils.buildUri(this.host, ServiceUriPaths.CORE_MANAGEMENT));
        double currentInsertCount = stat.get(
                ServiceHostManagementService.STAT_NAME_AUTHORIZATION_CACHE_INSERT_COUNT).latestValue;

        // Make a second request and verify that the cache did not get updated, instead Xenon re-used
        // the cached Guest authorization context.
        sender.sendAndWait(Operation.createGet(this.host, ExampleService.FACTORY_LINK));
        stat = this.host.getServiceStats(
                UriUtils.buildUri(this.host, ServiceUriPaths.CORE_MANAGEMENT));
        double newInsertCount = stat.get(
                ServiceHostManagementService.STAT_NAME_AUTHORIZATION_CACHE_INSERT_COUNT).latestValue;
        assertTrue(currentInsertCount == newInsertCount);

        // Make sure that Authorization Context cache in Xenon has at least one cached token.
        double currentCacheSize = stat.get(
                ServiceHostManagementService.STAT_NAME_AUTHORIZATION_CACHE_SIZE).latestValue;
        assertTrue(currentCacheSize == newInsertCount);
    }

    @Test
    public void testODLGetWithAuthorization() throws Throwable {
        long cacheDelayInterval = TimeUnit.MILLISECONDS.toMicros(100);
        this.host.setMaintenanceIntervalMicros(cacheDelayInterval);
        this.host.setServiceCacheClearDelayMicros(cacheDelayInterval);

        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        AuthorizationHelper authsetupHelper = new AuthorizationHelper(this.host);
        String email = "foo@foo.com";
        String userLink = authsetupHelper.createUserService(this.host, email);
        Query userGroupQuery = Query.Builder.create().addFieldClause(UserState.FIELD_NAME_EMAIL, email).build();
        String userGroupLink = authsetupHelper.createUserGroup(this.host, email, userGroupQuery);
        Query resourceGroupQuery = Query.Builder.create().addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, "*", MatchType.WILDCARD).build();
        String resourceGroupLink = authsetupHelper.createResourceGroup(this.host, email, resourceGroupQuery);

        EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE, ServiceOption.FACTORY_ITEM);
        // Start the factory service. it will be needed to start services on-demand
        MinimalFactoryTestService factoryService = new MinimalFactoryTestService();
        factoryService.setChildServiceCaps(caps);
        this.host.startServiceAndWait(factoryService, "service", null);
        // Start some test services
        List<Service> services = this.host.doThroughputServiceStart(this.serviceCount,
                MinimalTestService.class, this.host.buildMinimalTestState(), caps, null);
        // verify services have stopped
        this.host.waitFor("wait for services to stop.", () -> {
            for (Service service : services) {
                if (this.host.getServiceStage(service.getSelfLink()) != null) {
                    return false;
                }
            }
            return true;
        });

        this.host.assumeIdentity(userLink);
        this.host.sendAndWaitExpectFailure(
                Operation.createGet(UriUtils.buildUri(this.host, services.get(0).getSelfLink())));
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        authsetupHelper.createRole(this.host, userGroupLink, resourceGroupLink, EnumSet.of(Action.GET));
        // Assume identity, GET should now succeed
        this.host.assumeIdentity(userLink);
        this.host.sendAndWaitExpectSuccess(
                Operation.createGet(UriUtils.buildUri(this.host, services.get(0).getSelfLink())));
    }

    @Test
    public void testInvalidUserAndResourceGroup() throws Throwable {
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        AuthorizationHelper authsetupHelper = new AuthorizationHelper(this.host);
        String email = "foo@foo.com";
        String userLink = authsetupHelper.createUserService(this.host, email);
        Query userGroupQuery = Query.Builder.create().addFieldClause(UserState.FIELD_NAME_EMAIL, email).build();
        String userGroupLink = authsetupHelper.createUserGroup(this.host, email, userGroupQuery);
        authsetupHelper.createRole(this.host, userGroupLink, "foo", EnumSet.allOf(Action.class));
        // Assume identity
        this.host.assumeIdentity(userLink);
        this.host.sendAndWaitExpectSuccess(
                Operation.createGet(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK)));
        // set an invalid userGroupLink for the user
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        UserState patchUserState = new UserState();
        patchUserState.userGroupLinks = Collections.singleton("foo");
        this.host.sendAndWaitExpectSuccess(
                Operation.createPatch(UriUtils.buildUri(this.host, userLink)).setBody(patchUserState));
        this.host.assumeIdentity(userLink);
        this.host.sendAndWaitExpectSuccess(
                Operation.createGet(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK)));
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
            URI uri = UriUtils.buildUri(this.host, selfLink);
            Operation patchOperation =
                    Operation.createPatch(uri)
                        .setBody(exampleServices.get(uri))
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
    public void testAllowAndDenyRoles() throws Exception {
        // 1) Create example services state as the system user
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        ExampleServiceState state = createExampleServiceState("testExampleOK", 1L);

        Operation response = this.host.waitForResponse(
                Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                        .setBody(state));
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());
        state = response.getBody(ExampleServiceState.class);

        // 2) verify Jane cannot POST or GET
        assertAccess(Policy.DENY);

        // 3) build ALLOW role and verify access
        buildRole("AllowRole", Policy.ALLOW);
        assertAccess(Policy.ALLOW);

        // 4) build DENY role and verify access
        buildRole("DenyRole", Policy.DENY);
        assertAccess(Policy.DENY);

        // 5) build another ALLOW role and verify access
        buildRole("AnotherAllowRole", Policy.ALLOW);
        assertAccess(Policy.DENY);

        // 6) delete deny role and verify access
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        response = this.host.waitForResponse(Operation.createDelete(
                UriUtils.buildUri(this.host,
                        UriUtils.buildUriPath(RoleService.FACTORY_LINK, "DenyRole"))));
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());
        assertAccess(Policy.ALLOW);
    }

    private void buildRole(String roleName, Policy policy) {
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        TestContext ctx = this.host.testCreate(1);
        AuthorizationSetupHelper.create().setHost(this.host)
                .setRoleName(roleName)
                .setUserGroupQuery(Query.Builder.create()
                        .addCollectionItemClause(UserState.FIELD_NAME_EMAIL, "jane@doe.com")
                        .build())
                .setResourceQuery(Query.Builder.create()
                        .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                                ExampleService.FACTORY_LINK,
                                MatchType.PREFIX)
                        .build())
                .setVerbs(EnumSet.of(Action.POST, Action.PUT, Action.PATCH, Action.GET,
                        Action.DELETE))
                .setPolicy(policy)
                .setCompletion((authEx) -> {
                    if (authEx != null) {
                        ctx.failIteration(authEx);
                        return;
                    }
                    ctx.completeIteration();
                }).setupRole();
        this.host.testWait(ctx);
    }

    private void assertAccess(Policy policy) throws Exception {
        this.host.assumeIdentity(this.userServicePath);
        Operation response = this.host.waitForResponse(
                Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                        .setBody(createExampleServiceState("testExampleDeny", 2L)));
        if (policy == Policy.DENY) {
            assertEquals(Operation.STATUS_CODE_FORBIDDEN, response.getStatusCode());
        } else {
            assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());
        }

        response = this.host.waitForResponse(
                Operation.createGet(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK)));
        assertEquals(Operation.STATUS_CODE_OK, response.getStatusCode());
        ServiceDocumentQueryResult result = response.getBody(ServiceDocumentQueryResult.class);
        if (policy == Policy.DENY) {
            assertEquals(Long.valueOf(0L), result.documentCount);
        } else {
            assertNotNull(result.documentCount);
            assertNotEquals(Long.valueOf(0L), result.documentCount);
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
        TestContext ctx1 = this.host.testCreate(1);
        this.host.send(
                Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                        .setBody(state)
                        .setCompletion((o, e) -> {
                            if (o.getStatusCode() != Operation.STATUS_CODE_FORBIDDEN) {
                                String message = String.format("Expected %d, got %s",
                                        Operation.STATUS_CODE_FORBIDDEN,
                                        o.getStatusCode());
                                ctx1.failIteration(new IllegalStateException(message));
                                return;
                            }

                            ctx1.completeIteration();
                        }));
        this.host.testWait(ctx1);

        // issue a GET on a factory with no auth context, no documents should be returned
        TestContext ctx2 = this.host.testCreate(1);
        this.host.send(
                Operation.createGet(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx2.failIteration(new IllegalStateException(e));
                        return;
                    }
                    ServiceDocumentQueryResult res = o
                            .getBody(ServiceDocumentQueryResult.class);
                    if (!res.documentLinks.isEmpty()) {
                        String message = String.format("Expected 0 results; Got %d",
                                res.documentLinks.size());
                        ctx2.failIteration(new IllegalStateException(message));
                        return;
                    }

                    ctx2.completeIteration();
                }));
        this.host.testWait(ctx2);

        // Assume Jane's identity
        this.host.assumeIdentity(this.userServicePath);
        // add docs accessible by jane
        exampleServices.putAll(createExampleServices("jane"));

        verifyJaneAccess(exampleServices, null);

        // Execute get on factory trying to get all example services
        TestContext ctx3 = this.host.testCreate(1);
        final ServiceDocumentQueryResult[] factoryGetResult = new ServiceDocumentQueryResult[1];
        Operation getFactory = Operation.createGet(
                UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx3.failIteration(e);
                        return;
                    }

                    factoryGetResult[0] = o.getBody(ServiceDocumentQueryResult.class);
                    ctx3.completeIteration();
                });
        this.host.send(getFactory);
        this.host.testWait(ctx3);

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

        // test user impersonation
        this.host.setSystemAuthorizationContext();
        AuthzStatefulService s = new AuthzStatefulService();
        this.host.addPrivilegedService(AuthzStatefulService.class);

        AuthzState body = new AuthzState();
        body.userLink = this.userServicePath;
        this.host.startServiceAndWait(s, UUID.randomUUID().toString(), body);
        this.host.resetSystemAuthorizationContext();
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
    public void authCacheClearToken() throws Throwable {
        this.host.setSystemAuthorizationContext();
        AuthorizationHelper authHelperForFoo = new AuthorizationHelper(this.host);
        String email = "foo@foo.com";
        String fooUserLink = authHelperForFoo.createUserService(this.host, email);
        // spin up a privileged service to query for auth context
        MinimalTestService s = new MinimalTestService();
        this.host.addPrivilegedService(MinimalTestService.class);
        this.host.startServiceAndWait(s, UUID.randomUUID().toString(), null);
        this.host.resetSystemAuthorizationContext();

        AuthorizationContext authContext1 = assumeIdentityAndGetContext(fooUserLink, s, true);
        AuthorizationContext authContext2 = assumeIdentityAndGetContext(fooUserLink, s, true);
        assertNotNull(authContext1);
        assertNotNull(authContext2);

        this.host.setSystemAuthorizationContext();
        Operation clearAuthOp = new Operation();
        clearAuthOp.setUri(UriUtils.buildUri(this.host, fooUserLink));
        TestContext ctx = this.host.testCreate(1);
        clearAuthOp.setCompletion(ctx.getCompletion());
        AuthorizationCacheUtils.clearAuthzCacheForUser(s, clearAuthOp);
        clearAuthOp.complete();
        this.host.testWait(ctx);
        this.host.resetSystemAuthorizationContext();

        assertNull(this.host.getAuthorizationContext(s, authContext1.getToken()));
        assertNull(this.host.getAuthorizationContext(s, authContext2.getToken()));
    }

    @Test
    public void transactionWithAuth() throws Throwable {
        // assume system identity so we can create roles
        this.host.setSystemAuthorizationContext();
        String resourceGroupLink = this.authHelper.createResourceGroup(this.host,
                "transaction-group", Builder.create()
                        .addFieldClause(
                                ServiceDocument.FIELD_NAME_KIND,
                                Utils.buildKind(TransactionServiceState.class))
                        .build());
        this.authHelper.createRole(this.host, this.authHelper.getUserGroupLink(),
                resourceGroupLink, EnumSet.allOf(Action.class));
        this.host.resetAuthorizationContext();
        // assume identity as Jane and test to see if example service documents can be created
        this.host.assumeIdentity(this.userServicePath);
        String txid = TestTransactionUtils.newTransaction(this.host);
        OperationContext.setTransactionId(txid);
        createExampleServices("jane");
        boolean committed = TestTransactionUtils.commit(this.host, txid);
        assertTrue(committed);
        OperationContext.setTransactionId(null);
        ServiceDocumentQueryResult res = host.getFactoryState(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        assertEquals(Long.valueOf(this.serviceCount), res.documentCount);
        // next create docs and abort; these documents must  not be present
        txid = TestTransactionUtils.newTransaction(this.host);
        OperationContext.setTransactionId(txid);
        createExampleServices("jane");
        res = host.getFactoryState(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        assertEquals(Long.valueOf(2 * this.serviceCount), res.documentCount);
        boolean aborted = TestTransactionUtils.abort(this.host, txid);
        assertTrue(aborted);
        OperationContext.setTransactionId(null);
        res = host.getFactoryState(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
        assertEquals(Long.valueOf( this.serviceCount), res.documentCount);
    }

    @Test
    public void updateAuthzCache() throws Throwable {
        ExecutorService executor = null;
        try {
            this.host.setSystemAuthorizationContext();
            AuthorizationHelper authsetupHelper = new AuthorizationHelper(this.host);
            String email = "foo@foo.com";
            String userLink = authsetupHelper.createUserService(this.host, email);
            Query userGroupQuery = Query.Builder.create().addFieldClause(UserState.FIELD_NAME_EMAIL, email).build();
            String userGroupLink = authsetupHelper.createUserGroup(this.host, email, userGroupQuery);
            UserState patchState = new UserState();
            patchState.userGroupLinks = Collections.singleton(userGroupLink);
            this.host.sendAndWaitExpectSuccess(
                    Operation.createPatch(UriUtils.buildUri(this.host, userLink))
                    .setBody(patchState));
            TestContext ctx = this.host.testCreate(this.serviceCount);
            Service s = this.host.startServiceAndWait(MinimalTestService.class, UUID.randomUUID()
                    .toString());
            executor = this.host.allocateExecutor(s);
            this.host.resetSystemAuthorizationContext();
            for (int i = 0; i < this.serviceCount; i++) {
                this.host.run(executor, () -> {
                    String serviceName = UUID.randomUUID().toString();
                    try {
                        this.host.setSystemAuthorizationContext();
                        Query resourceQuery = Query.Builder.create().addFieldClause(ExampleServiceState.FIELD_NAME_NAME,
                                serviceName).build();
                        String resourceGroupLink = authsetupHelper.createResourceGroup(this.host, serviceName, resourceQuery);
                        authsetupHelper.createRole(this.host, userGroupLink, resourceGroupLink, EnumSet.allOf(Action.class));
                        this.host.resetSystemAuthorizationContext();
                        this.host.assumeIdentity(userLink);
                        ExampleServiceState exampleState = new ExampleServiceState();
                        exampleState.name = serviceName;
                        exampleState.documentSelfLink = serviceName;
                        // Issue: https://www.pivotaltracker.com/story/show/131520613
                        // We have a potential race condition in the code where the role
                        // created above is not being reflected in the auth context for
                        // the user; We are retrying the operation to mitigate the issue
                        // till we have a fix for the issue
                        for (int retryCounter = 0; retryCounter < 3; retryCounter++) {
                            try {
                                this.host.sendAndWaitExpectSuccess(
                                        Operation.createPost(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                                        .setBody(exampleState));
                                break;
                            } catch (Throwable t) {
                                this.host.log(Level.WARNING, "Error creating example service: " + t.getMessage());
                                if (retryCounter == 2) {
                                    ctx.fail(new IllegalStateException("Example service creation failed thrice"));
                                    return;
                                }
                            }
                        }
                        this.host.sendAndWaitExpectSuccess(
                                Operation.createDelete(UriUtils.buildUri(this.host,
                                        UriUtils.buildUriPath(ExampleService.FACTORY_LINK, serviceName))));
                        ctx.complete();
                    } catch (Throwable e) {
                        this.host.log(Level.WARNING, e.getMessage());
                        ctx.fail(e);
                    }
                });
            }
            this.host.testWait(ctx);
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
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
        authHelperForFoo.createRoles(this.host, email);
        // spin up a privileged service to query for auth context
        MinimalTestService s = new MinimalTestService();
        this.host.addPrivilegedService(MinimalTestService.class);
        this.host.startServiceAndWait(s, UUID.randomUUID().toString(), null);
        this.host.resetSystemAuthorizationContext();

        String userGroupLink = authHelperForFoo.getUserGroupLink();
        String resourceGroupLink = authHelperForFoo.getResourceGroupLink();
        String roleLink = authHelperForFoo.getRoleLink();


        // get the user group service and clear the authz cache
        assertNotNull(assumeIdentityAndGetContext(fooUserLink, s, true));
        this.host.setSystemAuthorizationContext();
        Operation getUserGroupStateOp =
                Operation.createGet(UriUtils.buildUri(this.host, userGroupLink));
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
        clearAuthOp = new Operation();
        ctx = this.host.testCreate(1);
        clearAuthOp.setCompletion(ctx.getCompletion());
        clearAuthOp.setUri(UriUtils.buildUri(this.host, resourceGroupLink));
        AuthorizationCacheUtils.clearAuthzCacheForResourceGroup(s, clearAuthOp);
        clearAuthOp.complete();
        this.host.testWait(ctx);
        this.host.resetSystemAuthorizationContext();
        assertNull(assumeIdentityAndGetContext(fooUserLink, s, false));

        // get the role service and clear the authz cache
        assertNotNull(assumeIdentityAndGetContext(fooUserLink, s, true));
        this.host.setSystemAuthorizationContext();
        Operation getRoleStateOp =
                Operation.createGet(UriUtils.buildUri(this.host, roleLink));
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
        clearAuthOp = new Operation();
        clearAuthOp.setUri(UriUtils.buildUri(this.host, fooUserLink));
        ctx = this.host.testCreate(1);
        clearAuthOp.setCompletion(ctx.getCompletion());
        AuthorizationCacheUtils.clearAuthzCacheForUser(s, clearAuthOp);
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
