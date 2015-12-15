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

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
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

public class AuthorizationHelper {
    public static final String USER_EMAIL = "jane@doe.com";
    public static final String USER_SERVICE_PATH =
            UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, USER_EMAIL);

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

    public String createUserService(ServiceHost target, String email) throws Throwable {
        return createUserService(this.host, target, email);
    }

    public Collection<String> createRoles(ServiceHost target) throws Throwable {
        final Integer concurrentTasks = 6;
        this.host.testStart(concurrentTasks);

        // Create user group for jane@doe.com
        String userGroupLink =
                createUserGroup(target, "janes-user-group", Builder.create()
                        .addFieldClause(
                                "email",
                                USER_EMAIL)
                        .build());

        // Create resource group for example service state
        String exampleServiceResourceGroupLink =
                createResourceGroup(target, "janes-resource-group", Builder.create()
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_KIND,
                                Utils.buildKind(ExampleServiceState.class))
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_NAME,
                                "jane")
                        .build());

        // Create resource group to allow access on ALL query tasks created by user
        String queryTaskResourceGroupLink =
                createResourceGroup(target, "any-query-task-resource-group", Builder.create()
                        .addFieldClause(
                                QueryTask.FIELD_NAME_KIND,
                                Utils.buildKind(QueryTask.class))
                        .addFieldClause(
                                QueryTask.FIELD_NAME_AUTH_PRINCIPAL_LINK,
                                USER_SERVICE_PATH)
                        .build());

        Collection<String> paths = new HashSet<>();

        // Create roles tying these together
        paths.add(createRole(target, userGroupLink, exampleServiceResourceGroupLink,
                new HashSet<>(Arrays.asList(Action.GET, Action.POST))));

        // Create another role with PATCH permission to test if we calculate overall permissions correctly across roles.
        paths.add(createRole(target, userGroupLink, exampleServiceResourceGroupLink,
                new HashSet<>(Arrays.asList(Action.PATCH))));

        // Create role authorizing access to the user's own query tasks
        paths.add(createRole(target, userGroupLink, queryTaskResourceGroupLink,
                new HashSet<>(Arrays.asList(Action.GET, Action.POST, Action.PATCH, Action.DELETE))));

        this.host.testWait();

        return paths;
    }

    public String createUserGroup(ServiceHost target, String name, Query q) {
        URI postUserGroupsUri =
                UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        String selfLink =
                UriUtils.extendUri(postUserGroupsUri, name).getPath();

        // Create user group
        UserGroupState userGroupState = new UserGroupState();
        userGroupState.documentSelfLink = selfLink;
        userGroupState.query = q;

        this.host.send(Operation
                .createPost(postUserGroupsUri)
                .setBody(userGroupState)
                .setCompletion(this.host.getCompletion()));
        return selfLink;
    }

    public String createResourceGroup(ServiceHost target, String name, Query q) {
        URI postResourceGroupsUri =
                UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);
        String selfLink =
                UriUtils.extendUri(postResourceGroupsUri, name).getPath();

        ResourceGroupState resourceGroupState = new ResourceGroupState();
        resourceGroupState.documentSelfLink = selfLink;
        resourceGroupState.query = q;

        this.host.send(Operation
                .createPost(postResourceGroupsUri)
                .setBody(resourceGroupState)
                .setCompletion(this.host.getCompletion()));
        return selfLink;
    }

    public String createRole(ServiceHost target, String userGroupLink, String resourceGroupLink, Set<Action> verbs) {
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

        RoleState roleState = new RoleState();
        roleState.documentSelfLink = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_ROLES, selfLink);
        roleState.userGroupLink = userGroupLink;
        roleState.resourceGroupLink = resourceGroupLink;
        roleState.verbs = verbs;
        roleState.policy = Policy.ALLOW;

        this.host.send(Operation
                .createPost(UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_ROLES))
                .setBody(roleState)
                .setCompletion(this.host.getCompletion()));

        return roleState.documentSelfLink;
    }
}
