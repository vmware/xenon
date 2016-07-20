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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
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

    private long taskCreationTimeMicros;

    private long taskCompletionTimeMicros;

    private boolean isFailureExpected;

    @Before
    public void setUp() {
        this.factoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_GRAPH_QUERIES);
        this.isFailureExpected = false;
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
        // valid depth, depth != stage count
        QueryTask q = QueryTask.Builder.create().setQuery(
                Query.Builder.create().addKindFieldClause(ExampleServiceState.class)
                        .build())
                .build();
        initialBrokenState = GraphQueryTask.Builder.create(2).addQueryStage(q).build();
        post.setBody(initialBrokenState);
        this.host.sendAndWaitExpectFailure(post, Operation.STATUS_CODE_BAD_REQUEST);
        // valid depth, 1 stage, currentDepth > 0
        q = QueryTask.Builder.create().setQuery(
                Query.Builder.create().addKindFieldClause(ExampleServiceState.class)
                        .build())
                .build();
        initialBrokenState = GraphQueryTask.Builder.create(1).addQueryStage(q).build();
        initialBrokenState.currentDepth = 12000;
        post.setBody(initialBrokenState);
        this.host.sendAndWaitExpectFailure(post, Operation.STATUS_CODE_BAD_REQUEST);
        // resultLimit set in first stage
        q = QueryTask.Builder.create().setQuery(
                Query.Builder.create().addKindFieldClause(ExampleServiceState.class)
                        .build())
                .build();
        q.querySpec.resultLimit = 2;
        QueryTask secondStage = QueryTask.Builder.create().setQuery(
                Query.Builder.create().addKindFieldClause(ExampleServiceState.class)
                        .build())
                .build();
        initialBrokenState = GraphQueryTask.Builder.create(2)
                .addQueryStage(q)
                .addQueryStage(secondStage).build();

        post.setBody(initialBrokenState);
        this.host.sendAndWaitExpectFailure(post, Operation.STATUS_CODE_BAD_REQUEST);
        // resultLimit set in second stage
        secondStage.querySpec.resultLimit = 2;
        initialBrokenState = GraphQueryTask.Builder.create(2)
                .addQueryStage(q)
                .addQueryStage(secondStage).build();

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

    @Test
    public void threeStageTreeGraph() throws Throwable {
        String name = UUID.randomUUID().toString();

        int stageCount = 3;
        // We will create a graph with N layers of the same services, each layer pointing to instances
        // to the next layer. They are all services of the same type. Its like a graph of
        // friend relationships: each document has a field, called friendLinks, pointing to
        // other instances of itself. In our case, we use QueryValidationServiceState.serviceLinks.
        // The query will essentially be a friends of friends graph query, 4 deep
        int recursionDepth = stageCount - 1;
        createQueryTargetServices(name, recursionDepth);

        GraphQueryTask initialState = createTreeGraphTask(stageCount, false);
        GraphQueryTask finalState = waitForTask(initialState);
        logGraphQueryThroughput(finalState);

        int[] resultCounts = {
                this.serviceCount,
                this.serviceCount * this.linkCount,
                this.serviceCount * this.linkCount * this.linkCount
        };
        verifyNStageResult(finalState, true, resultCounts);

        // direct task, same parameters
        finalState = createTreeGraphTask(stageCount, true);
        logGraphQueryThroughput(finalState);
        verifyNStageResult(finalState, true, resultCounts);

        QueryTask finishedFirstStage = Utils.clone(finalState.stages.get(0));
        // indirect task, same parameters, initial stage has results
        initialState = createTreeGraphTask(stageCount, finishedFirstStage, false);
        finalState = waitForTask(initialState);
        logGraphQueryThroughput(finalState);
        verifyNStageResult(finalState, true, resultCounts);

        // direct task, same parameters, initial stage has results
        finalState = createTreeGraphTask(stageCount, finishedFirstStage, true);
        logGraphQueryThroughput(finalState);
        verifyNStageResult(finalState, true, resultCounts);

        // direct task, same parameters, initial stage has paginated results. Task should
        // process just a single page worth and progress the page link.
        // Initial stage specifies SELECT_LINKS
        QueryTask stageWithResults = createGraphQueryStage(0);
        stageWithResults.querySpec.resultLimit = this.serviceCount / 2;
        createAndVerifyTreeGraphWithInitialStagePaginatedResults(stageCount, stageWithResults);

        // direct task, same parameters, initial stage has paginated results.
        // Initial stage does NOT specify QueryOption.SELECT_LINKS.
        // Expected failure
        this.isFailureExpected = true;
        stageWithResults = createGraphQueryStage(0);
        stageWithResults.querySpec.resultLimit = this.serviceCount / 2;
        stageWithResults.querySpec.options.remove(QueryOption.SELECT_LINKS);
        createAndVerifyTreeGraphWithInitialStagePaginatedResults(stageCount, stageWithResults);
        this.isFailureExpected = false;
    }

    private void createAndVerifyTreeGraphWithInitialStagePaginatedResults(int stageCount,
            QueryTask stageWithResults) throws Throwable {
        GraphQueryTask finalState;
        QueryTask finishedFirstStage;
        // wait for query task to finish. We will supply it in the *completed* stage, as part
        // of a graph query task, which will use its results (from the page link), instead
        // of executing its query
        URI firstStageTaskUri = this.host.createQueryTaskService(stageWithResults, true);
        finishedFirstStage = this.host.waitForQueryTask(firstStageTaskUri, TaskStage.FINISHED);
        finalState = createTreeGraphTask(stageCount, finishedFirstStage, true);

        if (this.isFailureExpected) {
            assertEquals(null, finalState);
            return;
        }

        logGraphQueryThroughput(finalState);
        // since the graph processed only a page worth, we expect less results per stage
        int pageLimit = stageWithResults.querySpec.resultLimit;
        int[] pagedResultCounts = {
                pageLimit,
                pageLimit * this.linkCount,
                pageLimit * this.linkCount * this.linkCount
        };

        verifyNStageResult(finalState, true, pagedResultCounts);
    }

    private void verifyNStageResult(GraphQueryTask finalState, int... expectedCounts) {
        verifyNStageResult(finalState, false, expectedCounts);
    }

    private void verifyNStageResult(GraphQueryTask finalState, boolean isRecursive,
            int... expectedCounts) {
        for (int i = 0; i < expectedCounts.length; i++) {
            int expectedCount = expectedCounts == null ? this.serviceCount : expectedCounts[i];
            ServiceDocumentQueryResult stageOneResults = finalState.stages.get(i).results;
            boolean isFinalStage = i == expectedCounts.length - 1;
            verifyStageResults(stageOneResults, i, expectedCount, isRecursive, isFinalStage);
        }
    }

    private void verifyStageResults(ServiceDocumentQueryResult stage,
            int expectedResultCount, boolean isFinalStage) {
        verifyStageResults(stage, 0, expectedResultCount, false, isFinalStage);
    }

    private void verifyStageResults(ServiceDocumentQueryResult stage,
            int stageIndex,
            int expectedResultCount, boolean isRecursive, boolean isFinalStage) {
        assertTrue(stage != null);
        assertTrue(stage.queryTimeMicros > 0);
        assertTrue(stage.documentCount == expectedResultCount);
        assertTrue(stage.documentLinks.size() == expectedResultCount);
        if (!isFinalStage && stage.selectedLinks == null) {
            if (expectedResultCount > 0) {
                throw new IllegalStateException("null selectedLinks");
            }
        } else if (!isFinalStage) {
            int expectedLinkCount = expectedResultCount;
            if (isRecursive) {
                expectedLinkCount *= this.linkCount;
            }
            assertTrue(stage.selectedLinks.size() == expectedLinkCount);
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
                .setDirect(true)
                .addQueryStage(stageOneSelectQueryValidationInstances)
                .addQueryStage(stageTwoSelectExampleInstances)
                .build();

        // set tenant links and verify each stage query task has the top level tenant links
        // transferred
        initialState.tenantLinks = new HashSet<>();
        initialState.tenantLinks.add("/some/link");
        initialState.tenantLinks.add("/some/other-link");
        initialState = createTask(initialState);
        for (QueryTask stage : initialState.stages) {
            assertTrue(stage.tenantLinks.size() == initialState.tenantLinks.size());
            for (String s : initialState.tenantLinks) {
                assertTrue(stage.tenantLinks.contains(s));
            }
        }
        return initialState;
    }

    private GraphQueryTask createTreeGraphTask(int stageCount,
            boolean isDirect) throws Throwable {
        return createTreeGraphTask(stageCount, null, isDirect);
    }

    private GraphQueryTask createTreeGraphTask(int stageCount,
            QueryTask initialStage,
            boolean isDirect) throws Throwable {
        GraphQueryTask initialState = createGraphTaskState(stageCount, initialStage);
        initialState.taskInfo = new TaskState();
        initialState.taskInfo.isDirect = isDirect;
        initialState = createTask(initialState);
        return initialState;
    }

    private GraphQueryTask createGraphTaskState(int stageCount, QueryTask initialStage) {
        GraphQueryTask.Builder builder = GraphQueryTask.Builder.create(stageCount);
        for (int i = 0; i < stageCount; i++) {
            if (i == 0 && initialStage != null) {
                builder.addQueryStage(initialStage);
                continue;
            }

            // each stage selects the services with the specific kind and the "serviceLinks" field
            // that points to more instances of the same service type. It logically forms a
            // directed graph, a tree, with the first layer pointing to serviceCount * linkCount
            // leafs, which in turn, each point to linkCount worth of sub leafs, etc
            QueryTask stage = createGraphQueryStage(i);
            builder.addQueryStage(stage);
        }

        GraphQueryTask initialState = builder.build();
        this.taskCreationTimeMicros = Utils.getNowMicrosUtc();
        return initialState;
    }

    private QueryTask createGraphQueryStage(int stageIndex) {
        QueryTask stage = QueryTask.Builder.create()
                .addOption(QueryOption.SELECT_LINKS)
                .addLinkTerm(QueryValidationServiceState.FIELD_NAME_SERVICE_LINKS)
                .setQuery(Query.Builder.create()
                        .addRangeClause(QueryValidationServiceState.FIELD_NAME_LONG_VALUE,
                                NumericRange.createLongRange((long) stageIndex, (long) stageIndex,
                                        true, true))
                        .addKindFieldClause(QueryValidationServiceState.class)
                        .build())
                .build();
        return stage;
    }

    private GraphQueryTask createTask(GraphQueryTask initialState)
            throws Throwable {
        Operation post = Operation.createPost(this.factoryUri);
        GraphQueryTask[] rsp = new GraphQueryTask[1];


        this.host.log("Creating task (isDirect:%s)", initialState.taskInfo.isDirect);
        TestContext ctx = testCreate(1);
        post.setBody(initialState).setCompletion((o, e) -> {
            if (e != null) {
                if (this.isFailureExpected) {
                    ctx.completeIteration();
                } else {
                    ctx.failIteration(e);
                }
                return;
            }
            GraphQueryTask r = o.getBody(GraphQueryTask.class);
            rsp[0] = r;
            if (initialState.taskInfo.isDirect) {
                this.taskCompletionTimeMicros = Utils.getNowMicrosUtc();
            }
            ctx.completeIteration();
        });

        // force remote to prove task results and response serialize properly
        post.forceRemote();
        this.host.send(post);
        testWait(ctx);
        if (this.isFailureExpected) {
            return null;
        }
        this.host.log("Task created (isDirect:%s) (stage: %s)",
                initialState.taskInfo.isDirect, rsp[0].taskInfo.stage);
        assertEquals(initialState.taskInfo.isDirect, rsp[0].taskInfo.isDirect);
        return rsp[0];
    }

    private GraphQueryTask waitForTask(GraphQueryTask initialState) throws Throwable {
        GraphQueryTask t = this.host.waitForFinishedTask(GraphQueryTask.class,
                initialState.documentSelfLink);
        this.taskCompletionTimeMicros = Utils.getNowMicrosUtc();
        TestContext ctx = testCreate(1);
        Operation get = Operation.createGet(this.host, initialState.documentSelfLink)
                .forceRemote().setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    GraphQueryTask rsp = o.getBody(GraphQueryTask.class);
                    if (rsp.stages == null) {
                        ctx.failIteration(new IllegalStateException("missing stages"));
                        return;
                    }
                    ctx.completeIteration();
                });
        this.host.send(get);
        testWait(ctx);
        return t;
    }

    private void logGraphQueryThroughput(GraphQueryTask finalState) {
        double timeDelta = this.taskCompletionTimeMicros - this.taskCreationTimeMicros;
        timeDelta = timeDelta / 1000000;
        double edgeCount = 0;
        double nodeCount = 0;
        for (QueryTask stage : finalState.stages) {
            if (stage.results != null && stage.results.selectedLinks != null) {
                edgeCount += stage.results.selectedLinks.size();
            }
            nodeCount += stage.results.documentCount;
        }
        double edgeTraversalThroughput = edgeCount / timeDelta;
        double nodeProcessingThroughput = nodeCount / timeDelta;
        this.host
                .log("IsDirect:%s, Edge count: %f, Node count: %f, Edge throughput: %f, Node throughput %f",
                        finalState.taskInfo.isDirect,
                        edgeCount, nodeCount,
                        edgeTraversalThroughput,
                        nodeProcessingThroughput);
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

        // to verify we do not include services NOT linked, create additional services not refered to
        // by the query validation service instances
        this.host.doFactoryChildServiceStart(null,
                this.serviceCount, ExampleServiceState.class,
                (o) -> {
                    ExampleServiceState s = new ExampleServiceState();
                    s.name = name;
                    s.id = UUID.randomUUID().toString();
                    o.setBody(s);
                }, UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));
    }

    /**
     * Creates N query validation services, linking their documents to example service instances.
     * These two sets of service documents form the document graph we will traverse during tests
     */
    private void startLinkedQueryTargetServices(
            Map<URI, ExampleServiceState> exampleStates, int recursionDepth)
            throws Throwable {
        Set<String> nextLayerLinks = new HashSet<>();
        Set<String> previousLayerLinks = new HashSet<>();

        this.host.log("Building document graph: Service count:%d, links per document:%d, layers:%d",
                this.serviceCount,
                this.linkCount,
                recursionDepth);

        for (int layer = 0; layer < recursionDepth + 1; layer++) {

            nextLayerLinks.clear();
            for (int i = 0; i < this.serviceCount * Math.pow(this.linkCount, layer + 1); i++) {
                nextLayerLinks.add(UUID.randomUUID().toString());
            }

            this.host.log(
                    "Graph vertex counts, next: %d, previous:%d, layer:%d",
                    nextLayerLinks.size(),
                    previousLayerLinks.size(),
                    layer);
            if (previousLayerLinks.isEmpty()) {
                for (int i = 0; i < this.serviceCount; i++) {
                    previousLayerLinks.add(UUID.randomUUID().toString());
                }
            }

            createGraphLayerOfLinkedServices(exampleStates, nextLayerLinks, previousLayerLinks,
                    layer);
            previousLayerLinks = new HashSet<>(nextLayerLinks);
        }
    }

    private void createGraphLayerOfLinkedServices(Map<URI, ExampleServiceState> exampleStates,
            Set<String> nextLayerLinks, Set<String> previousLayerLinks, int layer)
            throws Throwable {
        TestContext ctx = testCreate(previousLayerLinks.size());

        Iterator<String> nextLayerLinkIt = nextLayerLinks.iterator();
        Iterator<ExampleServiceState> exampleStateIt = exampleStates.values().iterator();
        for (String link : previousLayerLinks) {
            QueryValidationServiceState initState = new QueryValidationServiceState();
            initState.id = UUID.randomUUID().toString();
            if (!exampleStateIt.hasNext()) {
                exampleStateIt = exampleStates.values().iterator();
            }
            initState.longValue = (long) layer;
            initState.serviceLink = exampleStateIt.next().documentSelfLink;
            initState.serviceLinks = new ArrayList<>();
            for (int l = 0; l < this.linkCount; l++) {
                initState.serviceLinks.add(UriUtils.normalizeUriPath(nextLayerLinkIt.next()));
            }
            Operation post = Operation.createPost(this.host, link)
                    .setBody(initState)
                    .setCompletion(ctx.getCompletion());
            this.host.startService(post, new QueryValidationTestService());
        }
        testWait(ctx);
    }

}
