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
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.http.netty.NettyHttpServiceClient;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleFactoryService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.GuestUserService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.authn.AuthenticationRequest;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;

public class TestAuthorization extends BasicTestCase {

    String userServicePath;
    AuthorizationHelper authHelper;

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
        this.authHelper.createRoles(this.host);
        this.host.resetAuthorizationContext();
    }

    @Test
    public void testAuthPrincipalQuery() throws Throwable {
        this.host.assumeIdentity(this.userServicePath, null);
        createExampleServices("jane");
        this.host.createAndWaitSimpleDirectQuery(ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK,
                this.userServicePath, 2, 2);
    }

    @Test
    public void testScheduleAndRunContext() throws Throwable {
        this.host.assumeIdentity(this.userServicePath, null);

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
    public void testGuestAuthorization() throws Throwable {
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());

        this.host.testStart(3);

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

        this.host.testWait();

        // Create some example services; some accessible, some not
        Map<URI, ExampleServiceState> exampleServices = new HashMap<>();
        exampleServices.putAll(createExampleServices("jane"));
        exampleServices.putAll(createExampleServices("guest"));

        OperationContext.setAuthorizationContext(null);

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
        assertAuthorizedServicesInResult("guest", exampleServices, factoryGetResult[0]);

    }

    @Test
    public void honorVerbsInRoles() throws Throwable {

        // Assume Jane's identity
        this.host.assumeIdentity(this.userServicePath, null);

        // add docs accessible by jane
        Map<URI, ExampleServiceState> exampleServices = createExampleServices("jane");

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
        this.host.assumeIdentity(this.userServicePath, null);
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

    private String adminUser = "admim@localhost";
    private String exampleUser = "example@localhost";

    /**
     * Validate the AuthorizationSetupHelper
     */
    @Test
    public void testAuthSetupHelper() throws Throwable {
        // Step 1: Set up two users, one admin, one not.
        OperationContext.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        makeUsersWithAuthSetupHelper();
        OperationContext.setAuthorizationContext(null);

        // Step 2: Have each user login and get a token
        Map<String, String> adminCookies = loginUser(this.adminUser, this.adminUser);
        Map<String, String> exampleCookies = loginUser(this.exampleUser, this.exampleUser);

        // Step 3: Have each user create an example document
        createExampleDocument(adminCookies);
        createExampleDocument(exampleCookies);

        // Step 4: Verify that the admin can see both documents, but the non-admin
        // can only see the one it created
        assertTrue(numberExampleDocuments(adminCookies) == 2);
        assertTrue(numberExampleDocuments(exampleCookies) == 1);

        this.host.log("AuthorizationSetupHelper is working");
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

        this.host.testStart(2);
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
        this.host.testWait();
    }

    /**
     * Supports testAuthSetupHelper() by logging in a single user and returning the cookies
     * they get upon login
     */
    private Map<String, String> loginUser(String user, String password) throws Throwable {
        String basicAuth = constructBasicAuth(user, password);
        URI loginUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHN_BASIC);
        AuthenticationRequest login = new AuthenticationRequest();
        login.requestType = AuthenticationRequest.AuthenticationRequestType.LOGIN;

        Operation[] loginResponse = new Operation[1];

        Operation loginPost = Operation.createPost(loginUri)
                .setBody(login)
                .addRequestHeader(BasicAuthenticationService.AUTHORIZATION_HEADER_NAME,
                        basicAuth)
                .forceRemote()
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                        return;
                    }
                    loginResponse[0] = op;
                    this.host.completeIteration();
                });

        this.host.testStart(1);
        this.host.send(loginPost);
        this.host.testWait();

        // We extract the cookies here instead of in the completion
        // because it's awkward to make an array of hash tables
        Map<String, String> cookies = loginResponse[0].getCookies();
        assertTrue(cookies.size() > 0);

        return cookies;
    }

    /**
     * Supports testAuthSetupHelper() by creating an example document. The document
     * is created with a set of cookies as returned by the login above, so it is
     * created with a specific user.
     */
    private void createExampleDocument(Map<String, String> cookies) throws Throwable {
        ExampleServiceState example = new ExampleServiceState();
        example.name = UUID.randomUUID().toString();
        URI exampleUri = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK);

        Operation examplePost = Operation.createPost(exampleUri)
                .setBody(example)
                .forceRemote()
                .setCompletion(this.host.getCompletion());
        examplePost.setCookies(cookies);
        clearClientCookieJar();

        this.host.testStart(1);
        this.host.send(examplePost);
        this.host.testWait();
    }

    /**
     * Supports testAuthSetupHelper() by counting how many example documents we can
     * see with the given set of cookies (which correspond to a given user)
     */
    private int numberExampleDocuments(Map<String, String> cookies) throws Throwable {
        URI exampleUri = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK);

        // It would be nice to use getFactoryState, but we need to set the cookies so we can't
        Integer[] numberDocuments = new Integer[1];
        Operation get = Operation.createGet(exampleUri)
                .forceRemote()
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                        return;
                    }
                    ServiceDocumentQueryResult response = op.getBody(ServiceDocumentQueryResult.class);
                    assertTrue(response != null && response.documentLinks != null);
                    numberDocuments[0] = response.documentLinks.size();
                    this.host.completeIteration();
                });
        get.setCookies(cookies);
        clearClientCookieJar();

        this.host.testStart(1);
        this.host.send(get);
        this.host.testWait();
        return numberDocuments[0];
    }

    /**
     * Supports testAuthSetupHelper() by creating a Basic Auth header
     */
    private String constructBasicAuth(String name, String password) {
        String userPass = String.format("%s:%s", name, password);
        String encodedUserPass = new String(Base64.getEncoder().encode(userPass.getBytes()));
        String basicAuth = "Basic " + encodedUserPass;
        return basicAuth;

    }

    /**
     * Clear NettyHttpServiceClient's cookie jar
     *
     * The NettyHttpServiceClient is nice: it tracks what cookies we receive and sets them
     * on the outgoing connection. However, for some of our tests here, we want to control
     * the cookies. Therefore we clear the cookie jar.
     */
    private void clearClientCookieJar() {
        NettyHttpServiceClient client = (NettyHttpServiceClient) this.host.getClient();
        client.clearCookieJar();
    }
}
