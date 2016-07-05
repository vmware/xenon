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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;

public class TestGraphQueryTaskService extends BasicTestCase {
    private URI factoryUri;

    /**
     * Number of services in the top tier of the graph
     */
    public int serviceCount = 10;

    /**
     * Number of links to peer services, per service
     */
    public int linkCount = 2;

    @Before
    public void setUp() {
        this.factoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_GRAPH_QUERIES);
        CommandLineArgumentParser.parseFromProperties(this);
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
        initialBrokenState = GraphQueryTask.Builder.create(2).addQueryStage(q).build();
        initialBrokenState.currentDepth = 12000;
        post.setBody(initialBrokenState);
        this.host.sendAndWaitExpectFailure(post, Operation.STATUS_CODE_BAD_REQUEST);
    }

    @Test
    public void twoStageEmptyResults() throws Throwable {
        String name = UUID.randomUUID().toString();
        GraphQueryTask initialState = createTwoStageTask(name);
        GraphQueryTask finalState = waitForTask(initialState);
        // we do not expect results, since we never created any documents. But first stage
        // should have at least run and returned zero documents
        ServiceDocumentQueryResult stageOneResults = finalState.stages.get(0).results;
        verifyStageResults(stageOneResults, 0, true);

        verifyEmptyResultTask(finalState);

        // do the same for a direct task. Since its direct, creation should return final state
        initialState = createTwoStageTask(name, true);
        verifyEmptyResultTask(finalState);
    }

    @Test
    public void twoStage() throws Throwable {
        String name = UUID.randomUUID().toString();

        createQueryTargetServices(name, 0);

        GraphQueryTask initialState = createTwoStageTask(name);
        GraphQueryTask finalState = waitForTask(initialState);

        verifyNStageResult(finalState, this.serviceCount, this.serviceCount);

        finalState = createTwoStageTask(name, true);
        verifyNStageResult(finalState, this.serviceCount, this.serviceCount);
    }

    @Test
    public void twoStageNoResultsFinalStage() throws Throwable {
        String name = UUID.randomUUID().toString();

        createQueryTargetServices(name, 0);

        // delete the linked services, so our final stage produces zero results
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        GraphQueryTask initialState = createTwoStageTask(name);
        GraphQueryTask finalState = waitForTask(initialState);

        verifyNStageResult(finalState, this.serviceCount, 0);

        finalState = createTwoStageTask(name, true);
        verifyNStageResult(finalState, this.serviceCount, 0);
    }

    @Ignore // remove before CHECK IN
    @Test
    public void threeStageRecursive() throws Throwable {
        String name = UUID.randomUUID().toString();

        int stageCount = 3;
        // We will create a graph with N layers of the same services, each layer pointing to instances
        // to the next layer. They are all services of the same type. Its like a graph of
        // friend relationships: each document has a field, called friendLinks, pointing to
        // other instances of itself. In our case, we use QueryValidationServiceState.serviceLinks.
        // The query will essentially be a friends of friends graph query, 4 deep
        int recursionDepth = stageCount - 1;
        createQueryTargetServices(name, recursionDepth);

        GraphQueryTask initialState = createMultiStageRecursiveTask(name, stageCount, false);
        GraphQueryTask finalState = waitForTask(initialState);

        int firstTierServiceCount = this.serviceCount * this.linkCount;
        verifyNStageResult(finalState, this.serviceCount,
                firstTierServiceCount,
                firstTierServiceCount * firstTierServiceCount);

        finalState = createMultiStageRecursiveTask(name, stageCount, true);
        verifyNStageResult(finalState, this.serviceCount,
                firstTierServiceCount,
                firstTierServiceCount * firstTierServiceCount);

    }

    private void verifyNStageResult(GraphQueryTask finalState, int... expectedCounts) {
        for (int i = 0; i < expectedCounts.length; i++) {
            int expectedCount = expectedCounts == null ? this.serviceCount : expectedCounts[i];
            ServiceDocumentQueryResult stageOneResults = finalState.stages.get(i).results;
            boolean isFinalStage = i == expectedCounts.length - 1;
            verifyStageResults(stageOneResults, expectedCount, isFinalStage);
        }
    }

    private void verifyStageResults(ServiceDocumentQueryResult stage,
            int expectedResultCount, boolean isFinalStage) {
        assertTrue(stage != null);
        assertTrue(stage.queryTimeMicros > 0);
        assertTrue(stage.documentCount == expectedResultCount);
        assertTrue(stage.documentLinks.size() == expectedResultCount);
        if (!isFinalStage && stage.selectedLinks == null) {
            if (expectedResultCount > 0) {
                throw new IllegalStateException("null selectedLinks");
            }
        } else if (!isFinalStage) {
            assertTrue(stage.selectedLinks.size() == expectedResultCount);
        }
    }

    private void verifyEmptyResultTask(GraphQueryTask finalState) {
        // second stage should not even have run, since first stage had zero results
        ServiceDocumentQueryResult stageTwoResults = finalState.stages.get(1).results;
        assertTrue(stageTwoResults == null);

        assertEquals(1, finalState.resultLinks.size());
        assertTrue(finalState.resultLinks.get(0).startsWith(ServiceUriPaths.CORE_QUERY_TASKS));

        assertEquals(1, finalState.currentDepth);
    }

    private GraphQueryTask createTwoStageTask(String name) throws Throwable {
        return createTwoStageTask(name, false);
    }

    private GraphQueryTask createTwoStageTask(String name, boolean isDirect) throws Throwable {

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

        initialState = createTask(initialState, isDirect);
        return initialState;
    }

    private GraphQueryTask createMultiStageRecursiveTask(String name, int stageCount,
            boolean isDirect) throws Throwable {
        GraphQueryTask.Builder builder = GraphQueryTask.Builder.create(stageCount);
        for (int i = 0; i < stageCount; i++) {
            QueryTask stage = QueryTask.Builder.create()
                    .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS)
                    .setQuery(Query.Builder.create()
                            .addKindFieldClause(QueryValidationServiceState.class)
                            .build())
                    .build();
            builder.addQueryStage(stage);
        }

        GraphQueryTask initialState = builder.build();
        initialState = createTask(initialState, isDirect);
        return initialState;
    }

    private GraphQueryTask createTask(GraphQueryTask initialState, boolean isDirect)
            throws Throwable {
        Operation post = Operation.createPost(this.factoryUri);
        GraphQueryTask[] rsp = new GraphQueryTask[1];

        if (isDirect) {
            initialState.taskInfo = new TaskState();
            initialState.taskInfo.isDirect = isDirect;
        }

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

    private void createQueryTargetServices(String name, int recursionDepth) throws Throwable {
        Map<URI, ExampleServiceState> exampleStates = this.host.doFactoryChildServiceStart(null,
                this.serviceCount, ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState s = new ExampleServiceState();
                    s.name = name;
                    s.id = UUID.randomUUID().toString();
                    o.setBody(s);
                }, UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        startLinkedQueryTargetServices(exampleStates, recursionDepth);
    }

    /**
     * Creates N query validation services, linking their documents to example service instances.
     * These two sets of service documents form the document graph we will traverse during tests
     */
    private Collection<URI> startLinkedQueryTargetServices(
            Map<URI, ExampleServiceState> exampleStates, int recursionDepth)
            throws Throwable {
        Set<URI> uris = new ConcurrentSkipListSet<>();
        TestContext ctx = testCreate(exampleStates.size());
        for (ExampleServiceState exampleState : exampleStates.values()) {
            QueryValidationServiceState initState = new QueryValidationServiceState();
            initState.id = exampleState.id;
            initState.serviceLink = exampleState.documentSelfLink;
            Operation post = Operation.createPost(this.host, UUID.randomUUID().toString())
                    .setBody(initState)
                    .setCompletion(ctx.getCompletion());
            this.host.startService(post, new QueryValidationTestService());
        }
        testWait(ctx);
        return uris;
    }

}
