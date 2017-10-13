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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ResourceGroupService.PatchQueryRequest;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;

public class TestResourceGroupService extends BasicReusableHostTestCase {
    private URI factoryUri;

    @Before
    public void setUp() {
        this.factoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.deleteAllChildServices(this.factoryUri);
    }

    @Test
    public void testFactoryPost() throws Throwable {
        Query query = new Query();
        query.setTermPropertyName("name");
        query.setTermMatchValue("value");
        ResourceGroupState state = ResourceGroupState.Builder.create()
                .withQuery(query)
                .build();
        postHelper(state);
    }

    @Test
    public void testIdempotentPost() throws Throwable {
        Query query = new Query();
        query.setTermPropertyName("name");
        query.setTermMatchValue("value");
        ResourceGroupState state = ResourceGroupState.Builder.create()
                .withSelfLink("test-idemp-post")
                .withQuery(query)
                .build();
        postHelper(state);

        // Post to same service with new values.
        state.query.setTermPropertyName("new-name");
        state.query.setTermMatchValue("new-value");
        postHelper(state);
    }

    @Test
    public void testPatch() throws Throwable {
        Query query = new Query();
        query.setTermPropertyName("name");
        query.setTermMatchValue("value");
        ResourceGroupState state = ResourceGroupState.Builder.create()
                .withSelfLink("test-patch")
                .withQuery(query)
                .build();
        ResourceGroupState returnState = this.postOrPatchHelper(state, Action.POST, this.factoryUri);

        // issue a PATCH with a query clause to add
        Query clause =  new Query();
        clause.setTermPropertyName("patch-name");
        clause.setTermMatchValue("patch-value");
        clause.setNumericRange(NumericRange.createEqualRange(1L));
        PatchQueryRequest queryPatch = PatchQueryRequest.create(clause, false);
        returnState = this.postOrPatchHelper(queryPatch, Action.PATCH, UriUtils.buildUri(this.host, returnState.documentSelfLink));
        assertEquals(returnState.query.booleanClauses.size(), 1);

        // issue a PATCH with a query clause to remove an entry that does not exist
        queryPatch.clause.setNumericRange(NumericRange.createEqualRange(2L));
        queryPatch.removeClause = true;
        returnState = this.postOrPatchHelper(queryPatch, Action.PATCH, UriUtils.buildUri(this.host, returnState.documentSelfLink));
        assertEquals(returnState.query.booleanClauses.size(), 1);

        queryPatch.clause.setNumericRange(NumericRange.createEqualRange(1L));
        returnState = this.postOrPatchHelper(queryPatch, Action.PATCH, UriUtils.buildUri(this.host, returnState.documentSelfLink));
        assertEquals(returnState.query.booleanClauses.size(), 0);
    }

    @Test
    public void testFactoryIdempotentPost() throws Throwable {
        Query query = new Query();
        query.setTermPropertyName("name");
        query.setTermMatchValue("value");

        String servicePath = UriUtils.buildUriPath(ResourceGroupService.FACTORY_LINK, "my-resource-group");

        ResourceGroupState state = ResourceGroupState.Builder.create()
                .withSelfLink(servicePath)
                .withQuery(query)
                .build();

        ResourceGroupState responseState = this.host.verifyPost(ResourceGroupState.class,
                ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.query.term.propertyName, responseState.query.term.propertyName);
        assertEquals(state.query.term.matchValue, responseState.query.term.matchValue);

        long initialVersion = responseState.documentVersion;

        responseState = this.host.verifyPost(ResourceGroupState.class,
                ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.query.term.propertyName, responseState.query.term.propertyName);
        assertEquals(state.query.term.matchValue, responseState.query.term.matchValue);

        ResourceGroupState getState = this.sender.sendAndWait(Operation.createGet(this.host, servicePath), ResourceGroupState.class);
        assertEquals("version should not increase", initialVersion, getState.documentVersion);

        // modify state
        state.query.setTermMatchValue("valueModified");

        responseState = this.host.verifyPost(ResourceGroupState.class,
                ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.query.term.propertyName, responseState.query.term.propertyName);
        assertEquals(state.query.term.matchValue, responseState.query.term.matchValue);
        assertTrue("version should increase", initialVersion < responseState.documentVersion);
    }

    @Test
    public void testFactoryPostFailure() throws Throwable {
        ResourceGroupState state = ResourceGroupState.Builder.create()
                .withQuery(null)
                .build();

        Operation[] outOp = new Operation[1];
        Throwable[] outEx = new Throwable[1];

        Operation op = Operation.createPost(this.factoryUri)
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
        assertEquals("query is required", outEx[0].getMessage());
    }

    private void postHelper(ResourceGroupState state) throws Throwable {
        ResourceGroupState returnState = postOrPatchHelper(state, Action.POST, this.factoryUri);
        assertEquals(state.query.term.propertyName, returnState.query.term.propertyName);
        assertEquals(state.query.term.matchValue, returnState.query.term.matchValue);
    }

    private ResourceGroupState postOrPatchHelper(Object state, Action action, URI uri) throws Throwable {
        ResourceGroupState[] outState = new ResourceGroupState[1];

        Operation op = null;
        if (action.equals(Action.PATCH)) {
            op = Operation.createPatch(uri);
        } else {
            op = Operation.createPost(uri);
        }
        op.setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    outState[0] = o.getBody(ResourceGroupState.class);
                    this.host.completeIteration();
                });

        this.host.testStart(1);
        this.host.send(op);
        this.host.testWait();

        return outState[0];
    }
}
