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
import java.util.Collection;
import java.util.HashSet;

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

    public String createUserService(ServiceHost target, String email) throws Throwable {
        final String[] userUriPath = new String[1];

        UserState userState = new UserState();
        userState.documentSelfLink = email;
        userState.email = email;

        URI postUserUri = UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_USERS);
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

    public Collection<String> createRoles(ServiceHost target) throws Throwable {
        this.host.testStart(5);

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

        // Create resource group to allow GETs on ALL query tasks
        String queryTaskResourceGroupLink =
                createResourceGroup(target, "any-query-task-resource-group", Builder.create()
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_KIND,
                                Utils.buildKind(QueryTask.class))
                        .build());

        // Create roles tying these together
        Collection<String> paths = new HashSet();
        paths.add(createRole(target, userGroupLink, exampleServiceResourceGroupLink));
        paths.add(createRole(target, userGroupLink, queryTaskResourceGroupLink));

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

    public String createRole(ServiceHost target, String userGroupLink, String resourceGroupLink) {
        // Build selfLink from user group and resource group
        String userGroupSegment = userGroupLink.substring(userGroupLink.lastIndexOf('/') + 1);
        String resourceGroupSegment = resourceGroupLink.substring(resourceGroupLink.lastIndexOf('/') + 1);
        String selfLink = userGroupSegment + "-" + resourceGroupSegment;

        RoleState roleState = new RoleState();
        roleState.documentSelfLink = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_ROLES, selfLink);
        roleState.userGroupLink = userGroupLink;
        roleState.resourceGroupLink = resourceGroupLink;
        roleState.verbs = new HashSet<>();
        roleState.verbs.add(Action.GET);
        roleState.verbs.add(Action.POST);
        roleState.policy = Policy.ALLOW;

        this.host.send(Operation
                .createPost(UriUtils.buildUri(target, ServiceUriPaths.CORE_AUTHZ_ROLES))
                .setBody(roleState)
                .setCompletion(this.host.getCompletion()));

        return roleState.documentSelfLink;
    }
}
