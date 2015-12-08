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

package com.vmware.xenon.common.test.websockets;

import java.net.URI;
import java.util.HashSet;

import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.Policy;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;

/**
 * Tests websocket service with authentication on.
 */
public class TestAuthWebSocketService extends AbstractWebSocketServiceTest {
    private String userServicePath;

    @Override
    public void beforeHostStart(VerificationHost host) {
        // Enable authorization service; this is an end to end test
        host.setAuthorizationService(new AuthorizationContextService());
        host.setAuthorizationEnabled(true);
    }

    @Override
    public void setUp() throws Throwable {
        host.setSystemAuthorizationContext();
        this.userServicePath = this.host.createUserService("jane@doe.com");
        createRoles();
        super.setUp();
        host.resetAuthorizationContext();
        this.host.assumeIdentity(this.userServicePath, null);
    }

    @Test
    public void actions() throws Throwable {
        testGet();
        testPost();
        testPatch();
        testPut();
        testDelete();
    }

    @Test
    public void subscriptionLifecycle() throws Throwable {
        subscribeUnsubscribe("jane");
        subscribeStop("jane");
        subscribeClose("jane");
    }

    private void createRoles() throws Throwable {
        this.host.testStart(3);

        // Create user group for jane@doe.com
        String userGroupLink =
                createUserGroup("janes-user-group", Builder.create()
                        .addFieldClause(
                                "email",
                                "jane@doe.com")
                        .build());

        // Create resource group for example service state
        String exampleServiceResourceGroupLink =
                createResourceGroup("janes-resource-group", Builder.create()
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_KIND,
                                Utils.buildKind(ExampleServiceState.class))
                        .addFieldClause(
                                ExampleServiceState.FIELD_NAME_NAME,
                                "jane")
                        .build());

        // Create roles tying these together
        createRole(userGroupLink, exampleServiceResourceGroupLink);

        this.host.testWait();
    }

    private String createUserGroup(String name, Query q) {
        URI postUserGroupsUri =
                UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
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

    private String createResourceGroup(String name, Query q) {
        URI postResourceGroupsUri =
                UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);
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
}
