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
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;

public class TestGraphQueryTaskService extends BasicTestCase {
    private URI factoryUri;

    @Before
    public void setUp() {
        this.factoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_GRAPH_QUERIES);
    }

    @Test
    public void initialStateValidation() throws Throwable {
        // invalid depth
        GraphQueryTask initialBrokenState = GraphQueryTask.Builder.create(0).build();
        Operation post = Operation.createPost(this.factoryUri).setBody(initialBrokenState);
        this.host.sendAndWaitExpectFailure(post, Operation.STATUS_CODE_BAD_REQUEST);
        // valid depth, no stages
        initialBrokenState = GraphQueryTask.Builder.create(2).build();
        post.setBody(initialBrokenState);
        this.host.sendAndWaitExpectFailure(post, Operation.STATUS_CODE_BAD_REQUEST);
        // valid depth, 1 stage, currentDepth > 0
        QueryTask q = QueryTask.Builder.create().setQuery(
                Query.Builder.create().addKindFieldClause(ExampleServiceState.class)
                        .build())
                .build();
        initialBrokenState = GraphQueryTask.Builder.create(2).addQueryStage(q.querySpec).build();
        initialBrokenState.currentDepth = 12000;
        post.setBody(initialBrokenState);
        this.host.sendAndWaitExpectFailure(post, Operation.STATUS_CODE_BAD_REQUEST);
    }

    @Test
    public void twoStageEmptyResults() throws Throwable {
        String name = UUID.randomUUID().toString();

        // specify two key things:
        // The kind, so we begin the search from specific documents (the source nodes in the
        // graph), and the link, the graph edge that will lead us to documents in the second
        // stage
        QueryTask stageOneSelectQueryValidationInstances = QueryTask.Builder.create()
                .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINK)
                .setQuery(Query.Builder.create()
                        .addKindFieldClause(QueryValidationServiceState.class)
                        .build())
                .build();

        // for the second stage, filter the links by kind (although redundant, its good to
        // enforce the type of document we expect) and by a specific field value
        QueryTask stageTwoSelectExampleInstances = QueryTask.Builder.create()
                .setQuery(Query.Builder.create()
                        .addKindFieldClause(ExampleServiceState.class)
                        .addFieldClause(ExampleServiceState.FIELD_NAME_NAME, name)
                        .build())
                .build();

        GraphQueryTask initialState = GraphQueryTask.Builder.create(2)
                .addQueryStage(stageOneSelectQueryValidationInstances)
                .addQueryStage(stageTwoSelectExampleInstances)
                .build();

        initialState = createTask(initialState);
        GraphQueryTask finalState = waitForTask(initialState);
        // we do not expect results, since we never created any documents
        assertTrue(finalState.results.queryTimeMicros > 0);
        assertEquals(1, finalState.resultLinks.size());
        assertTrue(finalState.resultLinks.get(0).startsWith(ServiceUriPaths.CORE_QUERY_TASKS));
        assertEquals(0L, (long) finalState.results.documentCount);
        assertEquals(1, finalState.currentDepth);
    }

    private GraphQueryTask createTask(GraphQueryTask initialState) throws Throwable {
        Operation post = Operation.createPost(this.factoryUri);
        GraphQueryTask[] rsp = new GraphQueryTask[1];

        TestContext ctx = testCreate(1);
        post.setBody(initialState).setCompletion((o, e) -> {
            if (e != null) {
                ctx.failIteration(e);
                return;
            }
            GraphQueryTask r = o.getBody(GraphQueryTask.class);
            rsp[0] = r;
            ctx.completeIteration();
        });
        this.host.send(post);
        testWait(ctx);
        return rsp[0];
    }

    private GraphQueryTask waitForTask(GraphQueryTask initialState) throws Throwable {
        return this.host.waitForFinishedTask(GraphQueryTask.class, initialState.documentSelfLink);
    }

}
