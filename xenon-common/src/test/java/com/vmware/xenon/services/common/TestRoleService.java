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

package com.vmware.xenon.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.RoleService.Policy;
import com.vmware.xenon.services.common.RoleService.RoleState;

public class TestRoleService extends BasicReusableHostTestCase {
    private URI factoryUri;

    @Before
    public void setUp() {
        this.factoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_ROLES);
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.deleteAllChildServices(this.factoryUri);
    }

    RoleState validRoleState() {
        Set<Action> verbs = new HashSet<>();
        verbs.add(Action.GET);
        verbs.add(Action.POST);
        RoleState state = RoleState.Builder.create()
                .withUserGroupLink("/mock-user-group-link")
                .withResourceGroupLink("/mock-resource-group-link")
                .withVerbs(verbs)
                .withPolicy(Policy.ALLOW)
                .build();
        return state;
    }

    @Test
    public void testFactoryPost() throws Throwable {
        RoleState state = validRoleState();
        final RoleState[] outState = new RoleState[1];

        Operation op = Operation.createPost(this.factoryUri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    outState[0] = o.getBody(RoleState.class);
                    this.host.completeIteration();
                });

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();

        assertEquals(outState[0].userGroupLink, state.userGroupLink);
        assertEquals(outState[0].resourceGroupLink, state.resourceGroupLink);
    }

    @Test
    public void testFactoryIdempotentPost() throws Throwable {
        RoleState state = validRoleState();
        state.documentSelfLink = UUID.randomUUID().toString();

        RoleState responseState = (RoleState) this.host.verifyPost(RoleState.class,
                ServiceUriPaths.CORE_AUTHZ_ROLES,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.userGroupLink, responseState.userGroupLink);
        assertEquals(state.resourceGroupLink, responseState.resourceGroupLink);
        assertEquals(state.verbs, responseState.verbs);
        assertEquals(state.priority, responseState.priority);
        assertEquals(state.policy, responseState.policy);

        responseState = (RoleState) this.host.verifyPost(RoleState.class,
                ServiceUriPaths.CORE_AUTHZ_ROLES,
                state,
                Operation.STATUS_CODE_NOT_MODIFIED);

        assertEquals(state.userGroupLink, responseState.userGroupLink);
        assertEquals(state.resourceGroupLink, responseState.resourceGroupLink);
        assertEquals(state.verbs, responseState.verbs);
        assertEquals(state.priority, responseState.priority);
        assertEquals(state.policy, responseState.policy);

        state.verbs.add(Action.PATCH);

        responseState = (RoleState) this.host.verifyPost(RoleState.class,
                ServiceUriPaths.CORE_AUTHZ_ROLES,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.userGroupLink, responseState.userGroupLink);
        assertEquals(state.resourceGroupLink, responseState.resourceGroupLink);
        assertEquals(state.verbs, responseState.verbs);
        assertEquals(state.priority, responseState.priority);
        assertEquals(state.policy, responseState.policy);
    }

    void testFactoryPostFailure(Supplier<RoleState> sup) throws Throwable {
        RoleState state = sup.get();
        Operation[] outOp = new Operation[1];
        Throwable[] outEx = new Throwable[1];

        URI uri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        Operation op = Operation.createPost(uri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        outOp[0] = o;
                        outEx[0] = e;
                        this.host.completeIteration();
                        return;
                    }

                    // No exception, fail test
                    this.host.failIteration(new IllegalStateException("expected failure"));
                });

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();

        assertEquals(Operation.STATUS_CODE_FAILURE_THRESHOLD, outOp[0].getStatusCode());
        assertTrue(outEx[0].getMessage().matches("\\w+ is required"));
    }

    @Test
    public void testFactoryPostFailure() throws Throwable {
        testFactoryPostFailure(() -> {
            RoleState state = validRoleState();
            state.userGroupLink = null;
            return state;
        });

        testFactoryPostFailure(() -> {
            RoleState state = validRoleState();
            state.resourceGroupLink = null;
            return state;
        });

        testFactoryPostFailure(() -> {
            RoleState state = validRoleState();
            state.verbs = null;
            return state;
        });

        testFactoryPostFailure(() -> {
            RoleState state = validRoleState();
            state.policy = null;
            return state;
        });

        testFactoryPostFailure(() -> {
            RoleState state = validRoleState();
            state.policy = Policy.DENY;
            return state;
        });
    }
}
