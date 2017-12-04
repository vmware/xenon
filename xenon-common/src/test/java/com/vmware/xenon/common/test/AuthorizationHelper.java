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

package com.vmware.xenon.common.test;

import static org.junit.Assert.assertTrue;

import static com.vmware.xenon.services.common.authn.BasicAuthenticationUtils.constructBasicAuth;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.Policy;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;
import com.vmware.xenon.services.common.UserService.UserState;
import com.vmware.xenon.services.common.authn.AuthenticationRequest;

/**
 * Consider using {@link com.vmware.xenon.common.AuthorizationSetupHelper}
 */
public class AuthorizationHelper {

    private String userGroupLink;
    private String resourceGroupLink;
    private String roleLink;

    VerificationHost host;

    public AuthorizationHelper(VerificationHost host) {
        this.host = host;
    }

    public static String createUserService(VerificationHost host, ServiceHost target, String email) throws Throwable {
        final String[] userUriPath = new String[1];

        UserState userState = new UserState();
        userState.documentSelfLink = email;
        userState.email = email;

        URI postUserUri = UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_USERS);
        host.testStart(1);
        host.send(Operation
                .createPost(postUserUri)
                .setBody(userState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        host.failIteration(e);
                        return;
                    }
                    UserState state = o.getBody(UserState.class);
                    userUriPath[0] = state.documentSelfLink;
                    host.completeIteration();
                }));
        host.testWait();
        return userUriPath[0];
    }

    public void patchUserService(ServiceHost target, String userServiceLink, UserState userState) throws Throwable {
        URI patchUserUri = UriUtils.buildUri(target, userServiceLink);
        this.host.testStart(1);
        this.host.send(Operation
                .createPatch(patchUserUri)
                .setBody(userState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                }));
        this.host.testWait();
    }

    /**
     * Find user document and return the path.
     *   ex: /core/authz/users/sample@vmware.com
     *
     * @see VerificationHost#assumeIdentity(String)
     */
    public String findUserServiceLink(String userEmail) throws Throwable {
        Query userQuery = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND, Utils.buildKind(UserState.class))
                .addFieldClause(UserState.FIELD_NAME_EMAIL, userEmail)
                .build();

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(userQuery)
                .build();

        URI queryTaskUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_QUERY_TASKS);

        String[] userServiceLink = new String[1];

        TestContext ctx = this.host.testCreate(1);
        Operation postQuery = Operation.createPost(queryTaskUri)
                .setBody(queryTask)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        ctx.failIteration(ex);
                        return;
                    }
                    QueryTask queryResponse = op.getBody(QueryTask.class);
                    int resultSize = queryResponse.results.documentLinks.size();
                    if (queryResponse.results.documentLinks.size() != 1) {
                        String msg = String
                                .format("Could not find user %s, found=%d", userEmail, resultSize);
                        ctx.failIteration(new IllegalStateException(msg));
                        return;
                    } else {
                        userServiceLink[0] = queryResponse.results.documentLinks.get(0);
                    }
                    ctx.completeIteration();
                });
        this.host.send(postQuery);
        this.host.testWait(ctx);

        return userServiceLink[0];
    }

    /**
     * Call BasicAuthenticationService and returns auth token.
     */
    public String login(String email, String password) throws Throwable {
        String basicAuth = constructBasicAuth(email, password);
        URI loginUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHN_BASIC);
        AuthenticationRequest login = new AuthenticationRequest();
        login.requestType = AuthenticationRequest.AuthenticationRequestType.LOGIN;

        String[] authToken = new String[1];

        TestContext ctx = this.host.testCreate(1);

        Operation loginPost = Operation.createPost(loginUri)
                .setBody(login)
                .addRequestHeader(Operation.AUTHORIZATION_HEADER, basicAuth)
                .forceRemote()
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        ctx.failIteration(ex);
                        return;
                    }
                    authToken[0] = op.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER);
                    if (authToken[0] == null) {
                        ctx.failIteration(
                                new IllegalStateException("Missing auth token in login response"));
                        return;
                    }
                    ctx.completeIteration();
                });

        this.host.send(loginPost);
        this.host.testWait(ctx);

        assertTrue(authToken[0] != null);

        return authToken[0];

    }

    public void setUserGroupLink(String userGroupLink) {
        this.userGroupLink = userGroupLink;
    }

    public void setResourceGroupLink(String resourceGroupLink) {
        this.resourceGroupLink = resourceGroupLink;
    }

    public void setRoleLink(String roleLink) {
        this.roleLink = roleLink;
    }

    public String getUserGroupLink() {
        return this.userGroupLink;
    }

    public String getResourceGroupLink() {
        return this.resourceGroupLink;
    }

    public String getRoleLink() {
        return this.roleLink;
    }

    public String createUserService(ServiceHost target, String email) throws Throwable {
        return createUserService(this.host, target, email);
    }

    public String getUserGroupName(String email) {
        String emailPrefix = email.substring(0, email.indexOf("@"));
        return emailPrefix + "-user-group";
    }

    public Collection<String> createRoles(ServiceHost target, String email) throws Throwable {
        String emailPrefix = email.substring(0, email.indexOf("@"));

        // Create user group
        String userGroupLink = createUserGroup(target, getUserGroupName(email),
                Builder.create().addFieldClause("email", email).build());
        setUserGroupLink(userGroupLink);

        // Create resource group for example service state
        String exampleServiceResourceGroupLink =
                createResourceGroup(target, emailPrefix + "-resource-group", Builder.create()
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_KIND,
                                Utils.buildKind(ExampleServiceState.class))
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_NAME,
                                emailPrefix)
                        .build());
        setResourceGroupLink(exampleServiceResourceGroupLink);
        // Create resource group to allow access on ALL query tasks created by user
        String queryTaskResourceGroupLink =
                createResourceGroup(target, "any-query-task-resource-group", Builder.create()
                        .addFieldClause(
                                QueryTask.FIELD_NAME_KIND,
                                Utils.buildKind(QueryTask.class))
                        .addFieldClause(
                                QueryTask.FIELD_NAME_AUTH_PRINCIPAL_LINK,
                                UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, email))
                        .build());

        // Create resource group to allow access on utility paths
        String statsResourceGroupLink = createResourceGroup(target, "stats-resource-group",
                Builder.create()
                        .addFieldClause(
                                ServiceDocument.FIELD_NAME_SELF_LINK,
                                ExampleService.FACTORY_LINK + ServiceHost.SERVICE_URI_SUFFIX_STATS)
                        .build());

        String subscriptionsResourceGroupLink = createResourceGroup(target, "subs-resource-group",
                Builder.create()
                        .addFieldClause(
                                ServiceDocument.FIELD_NAME_SELF_LINK,
                                ServiceUriPaths.CORE_LOCAL_QUERY_TASKS
                                        + ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS)
                        .build());

        Collection<String> paths = new HashSet<>();

        // Create roles tying these together
        String exampleRoleLink = createRole(target, userGroupLink, exampleServiceResourceGroupLink,
                new HashSet<>(Arrays.asList(Action.GET, Action.POST)));
        setRoleLink(exampleRoleLink);
        paths.add(exampleRoleLink);
        // Create another role with PATCH permission to test if we calculate overall permissions correctly across roles.
        paths.add(createRole(target, userGroupLink, exampleServiceResourceGroupLink,
                new HashSet<>(Collections.singletonList(Action.PATCH))));

        // Create role authorizing access to the user's own query tasks
        paths.add(createRole(target, userGroupLink, queryTaskResourceGroupLink,
                new HashSet<>(Arrays.asList(Action.GET, Action.POST, Action.PATCH, Action.DELETE))));

        // Create role authorizing access to /stats
        paths.add(createRole(target, userGroupLink, statsResourceGroupLink,
                new HashSet<>(
                        Arrays.asList(Action.GET, Action.POST, Action.PATCH, Action.DELETE))));

        // Create role authorizing access to /subscriptions of query tasks
        paths.add(createRole(target, userGroupLink, subscriptionsResourceGroupLink,
                new HashSet<>(
                        Arrays.asList(Action.GET, Action.POST, Action.PATCH, Action.DELETE))));
        return paths;
    }

    public String createUserGroup(ServiceHost target, String name, Query q) throws Throwable {
        URI postUserGroupsUri =
                UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        String selfLink =
                UriUtils.extendUri(postUserGroupsUri, name).getPath();

        // Create user group
        UserGroupState userGroupState = UserGroupState.Builder.create()
                .withSelfLink(selfLink)
                .withQuery(q)
                .build();

        this.host.sendAndWaitExpectSuccess(Operation
                .createPost(postUserGroupsUri)
                .setBody(userGroupState));
        return selfLink;
    }

    public String createResourceGroup(ServiceHost target, String name, Query q) throws Throwable {
        URI postResourceGroupsUri =
                UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);
        String selfLink =
                UriUtils.extendUri(postResourceGroupsUri, name).getPath();

        ResourceGroupState resourceGroupState = ResourceGroupState.Builder.create()
                .withSelfLink(selfLink)
                .withQuery(q)
                .build();
        this.host.sendAndWaitExpectSuccess(Operation
                .createPost(postResourceGroupsUri)
                .setBody(resourceGroupState));
        return selfLink;
    }

    public String createRole(ServiceHost target, String userGroupLink, String resourceGroupLink, Set<Action> verbs) throws Throwable {
        // Build selfLink from user group, resource group, and verbs
        String userGroupSegment = userGroupLink.substring(userGroupLink.lastIndexOf('/') + 1);
        String resourceGroupSegment = resourceGroupLink.substring(resourceGroupLink.lastIndexOf('/') + 1);
        String verbSegment = "";
        for (Action a : verbs) {
            if (verbSegment.isEmpty()) {
                verbSegment = a.toString();
            } else {
                verbSegment += "+" + a.toString();
            }
        }
        String selfLink = userGroupSegment + "-" + resourceGroupSegment + "-" + verbSegment;

        RoleState roleState = RoleState.Builder.create()
                .withSelfLink(UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_ROLES, selfLink))
                .withUserGroupLink(userGroupLink)
                .withResourceGroupLink(resourceGroupLink)
                .withVerbs(verbs)
                .withPolicy(Policy.ALLOW)
                .build();
        this.host.sendAndWaitExpectSuccess(Operation
                .createPost(UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_ROLES))
                .setBody(roleState));
        return roleState.documentSelfLink;
    }
}
