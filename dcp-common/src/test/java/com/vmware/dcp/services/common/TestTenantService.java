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

package com.vmware.dcp.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.EnumSet;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.BasicReusableHostTestCase;
import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceDocumentQueryResult;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.common.test.TestProperty;

public class TestTenantService extends BasicReusableHostTestCase {
    public static final int SERVICE_COUNT = 100;
    private URI factoryURI;

    @Before
    public void setUp() throws Exception {
        this.factoryURI = UriUtils.buildUri(this.host, TenantFactoryService.class);
    }

    @After
    public void tearDown() throws Throwable {
        // delete all services
        this.host.deleteAllChildServices(this.factoryURI);
    }

    @Test
    public void testFactoryPost() throws Throwable {
        ServiceDocumentQueryResult res = createInstances(SERVICE_COUNT, false);
        assertTrue(res.documentLinks.size() == SERVICE_COUNT);
        assertTrue(res.documentLinks.size() == res.documents.size());

        TenantService.TenantState initialState = buildInitialState();

        for (Object s : res.documents.values()) {
            TenantService.TenantState state = Utils.fromJson(s, TenantService.TenantState.class);
            assertEquals(state.name, initialState.name);
            assertEquals(state.parentLink, initialState.parentLink);
            assertNotNull(state.id);
            assertTrue(state.documentSelfLink.endsWith(state.id));
        }
    }

    @Test
    public void testFactoryPostWithoutId() throws Throwable {
        ServiceDocumentQueryResult res = createInstances(SERVICE_COUNT, true);
        assertTrue(res.documentLinks.size() == SERVICE_COUNT);
        assertTrue(res.documentLinks.size() == res.documents.size());

        for (Object s : res.documents.values()) {
            TenantService.TenantState state = Utils.fromJson(s, TenantService.TenantState.class);
            assertNotNull(state.id);
            assertTrue(state.documentSelfLink.endsWith(state.id));
        }
    }

    @Test
    public void testPatch() throws Throwable {
        ServiceDocumentQueryResult initialStates = createInstances(SERVICE_COUNT, false);

        TenantService.TenantState patchBody = new TenantService.TenantState();
        patchBody.name = "tenantB";
        patchBody.parentLink = "superTenantB";
        patchBody.id = UUID.randomUUID().toString();
        doPatch(EnumSet.of(TestProperty.FORCE_REMOTE), SERVICE_COUNT, initialStates, patchBody);

        patchBody.name = "tenantC";
        patchBody.parentLink = "superTenantC";
        doPatch(EnumSet.of(TestProperty.FORCE_REMOTE), SERVICE_COUNT, initialStates, patchBody);

        ServiceDocumentQueryResult afterStates = this.host.getFactoryState(UriUtils
                .buildExpandLinksQueryUri(this.factoryURI));
        for (Object s : afterStates.documents.values()) {
            TenantService.TenantState state = Utils.fromJson(s, TenantService.TenantState.class);
            TenantService.TenantState initialState = Utils.fromJson(
                    initialStates.documents.get(state.documentSelfLink),
                    TenantService.TenantState.class);
            assertEquals(patchBody.name, state.name);
            assertEquals(patchBody.parentLink, state.parentLink);
            assertNotEquals(patchBody.id, state.id);
            assertEquals(initialState.id, state.id);
        }
    }

    public void doPatch(EnumSet<TestProperty> props, int c,
            ServiceDocumentQueryResult initialStates,
            TenantService.TenantState patchBody) throws Throwable {
        this.host.testStart(c);
        for (String link : initialStates.documentLinks) {
            Operation patch = Operation.createPatch(UriUtils.buildUri(this.host, link))
                    .setBody(patchBody)
                    .setCompletion(this.host.getCompletion());
            if (props.contains(TestProperty.FORCE_REMOTE)) {
                patch.forceRemote();
            }
            this.host.send(patch);
        }
        this.host.testWait();
    }

    private TenantService.TenantState buildInitialState() {
        TenantService.TenantState state = new TenantService.TenantState();
        state.name = "tenantA";
        state.id = UUID.randomUUID().toString();
        state.parentLink = UriUtils.buildUriPath(this.factoryURI.getPath(), "superTenantA");
        return state;
    }

    private ServiceDocumentQueryResult createInstances(int c, boolean nullId) throws Throwable {
        this.host.testStart(c);
        for (int i = 0; i < c; i++) {
            TenantService.TenantState initialState = buildInitialState();
            if (nullId) {
                initialState.id = null;
            }
            Operation startPost = Operation.createPost(this.factoryURI)
                    .setBody(initialState)
                    .setCompletion(this.host.getCompletion());
            this.host.send(startPost);
        }
        this.host.testWait();

        ServiceDocumentQueryResult res = this.host.getFactoryState(UriUtils
                .buildExpandLinksQueryUri(this.factoryURI));
        return res;
    }

}
