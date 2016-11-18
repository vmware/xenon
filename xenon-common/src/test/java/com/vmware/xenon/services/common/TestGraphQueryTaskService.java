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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
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
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.GraphQueryTask.GraphQueryOption;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;

public class TestGraphQueryTaskService extends BasicTestCase {
    private URI graphQueryFactoryUri;
    private URI exampleFactoryUri;

    /**
     * Number of services in the top tier of the graph
     */
    public int serviceCount = 10;

    /**
     * Number of links to peer services, per service
     */
    public int linkCount = 2;

    public int nodeCount = 3;

    private long taskCreationTimeMicros;

    private long taskCompletionTimeMicros;

    private boolean isFailureExpected;
    private URI queryTargetFactoryUri;
    private URI queryFactoryUri;

    @Before
    public void setUp() {
        this.graphQueryFactoryUri = UriUtils
                .buildUri(this.host, ServiceUriPaths.CORE_GRAPH_QUERIES);
        this.queryFactoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_QUERY_TASKS);
        this.exampleFactoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        this.queryTargetFactoryUri = UriUtils.buildUri(this.host,
                GraphQueryValidationTestService.FACTORY_LINK);
        this.host.startFactory(new GraphQueryValidationTestService());
        this.isFailureExpected = false;
        CommandLineArgumentParser.parseFromProperties(this);
    }

    private void setUpMultiNode() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);

        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.startFactory(new GraphQueryValidationTestService());
        }

        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.graphQueryFactoryUri = UriUtils.buildUri(
                this.host.getPeerServiceUri(ServiceUriPaths.CORE_GRAPH_QUERIES));
        this.queryFactoryUri = this.host
                .getPeerServiceUri(ServiceUriPaths.CORE_QUERY_TASKS);
        this.exampleFactoryUri = UriUtils.buildUri(
                this.host.getPeerServiceUri(ExampleService.FACTORY_LINK));
        this.queryTargetFactoryUri = UriUtils.buildUri(
                this.host.getPeerServiceUri(GraphQueryValidationTestService.FACTORY_LINK));
        this.host.waitForReplicatedFactoryServiceAvailable(this.graphQueryFactoryUri);
        this.host.waitForReplicatedFactoryServiceAvailable(this.exampleFactoryUri);
        this.host.waitForReplicatedFactoryServiceAvailable(this.queryTargetFactoryUri);
    }

    @After
    public void tearDown() {
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }

    @Test
    public void initialStateValidation() throws Throwable {
        // invalid depth
        GraphQueryTask initialBrokenState = GraphQueryTask.Builder.create(0).build();
        Operation post = Operation.createPost(this.graphQueryFactoryUri)
                .setBody(initialBrokenState);
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
        validateStageResults(stageOneResults, 0, true);

        validateEmptyResultTask(finalState);

        // do the same for a direct task. Since its direct, creation should return final state
        initialState = createTwoStageTask(name, true);
        validateEmptyResultTask(finalState);
    }

    @Test
    public void twoStage() throws Throwable {
        String name = UUID.randomUUID().toString();

        createQueryTargetServices(name, 0);

        GraphQueryTask initialState = createTwoStageTask(name);
        GraphQueryTask finalState = waitForTask(initialState);

        validateNStageResult(finalState, this.serviceCount, this.serviceCount);

        finalState = createTwoStageTask(name, true);
        validateNStageResult(finalState, this.serviceCount, this.serviceCount);
    }

    @Test
    public void twoStageNoResultsFinalStage() throws Throwable {
        String name = UUID.randomUUID().toString();

        createQueryTargetServices(name, 0);

        // delete the linked services, so our final stage produces zero results
        this.host.deleteAllChildServices(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK));

        GraphQueryTask initialState = createTwoStageTask(name);
        GraphQueryTask finalState = waitForTask(initialState);

        validateNStageResult(finalState, this.serviceCount, 0);

        finalState = createTwoStageTask(name, true);
        validateNStageResult(finalState, this.serviceCount, 0);
    }

    @Test
    public void threeStageTreeGraphMultiNode() throws Throwable {
        setUpMultiNode();
        verifyThreeStageTreeGraph();
    }

    @Test
    public void threeStageTreeGraph() throws Throwable {
        verifyThreeStageTreeGraph();
    }

    /**
     * Test various combination of direct = {true | false} and initial stage with | without
     *  computed results, on a three stage tree graph
     */
    public void verifyThreeStageTreeGraph() throws Throwable {
        String name = UUID.randomUUID().toString();

        int stageCount = 3;

        int recursionDepth = stageCount - 1;
        createQueryTargetServices(name, recursionDepth);

        GraphQueryTask finalState;
        int[] resultCounts = {
                this.serviceCount,
                this.serviceCount * this.linkCount,
                this.serviceCount * this.linkCount * this.linkCount
        };

        createAndVerifyTreeGraph(resultCounts, stageCount);

        createAndVerifyTreeGraphWithStageFiltering(stageCount);

        finalState = createAndVerifyTreeGraphDirect(stageCount, resultCounts);
        QueryTask finishedFirstStage = Utils.clone(finalState.stages.get(0));

        createAndVerifyTreeGraphWithZeroStageResults(
                stageCount, finishedFirstStage, resultCounts);

        createAndVerifyDirectTreeGraphZeroStageResults(stageCount, resultCounts, finishedFirstStage);

        // direct task, same parameters, initial stage has paginated results. Task should
        // process just a single page worth and progress the page link.
        // Initial stage specifies SELECT_LINKS
        QueryTask stageWithResults = createGraphQueryStage(0);
        stageWithResults.querySpec.resultLimit = this.serviceCount / 2;
        createAndVerifyDirectTreeGraphWithZeroStagePaginatedResults(stageCount, stageWithResults);

        // direct task, same parameters, initial stage has paginated results.
        // Initial stage does NOT specify QueryOption.SELECT_LINKS.
        // Expected failure
        this.isFailureExpected = true;
        stageWithResults = createGraphQueryStage(0);
        stageWithResults.querySpec.resultLimit = this.serviceCount / 2;
        stageWithResults.querySpec.options.remove(QueryOption.SELECT_LINKS);
        createAndVerifyDirectTreeGraphWithZeroStagePaginatedResults(stageCount, stageWithResults);
        this.isFailureExpected = false;
    }

    private void createAndVerifyTreeGraph(int[] resultCounts, int stageCount) throws Throwable {
        this.host.waitFor("query result mismatch", () -> {
            GraphQueryTask initialState = createTreeGraphTask(stageCount, false);
            GraphQueryTask finalState = waitForTask(initialState);
            logGraphQueryThroughput(finalState);
            return validateNStageResult(finalState, true, resultCounts);
        });
    }

    private void createAndVerifyTreeGraphWithStageFiltering(int stageCount) throws Throwable {
        // QueryOption.FILTER_STAGE_RESULTS will prune all document links from
        // stage N-1, that did not contribute to results in stage N
        GraphQueryTask initialState = createGraphTaskState(stageCount,
                EnumSet.of(GraphQueryOption.FILTER_STAGE_RESULTS), null);
        initialState.taskInfo = new TaskState();
        initialState.taskInfo.isDirect = true;

        // The test code created a set of target service documents that form a fully
        // connected tree: each layer N, is fully connected to layer N+1. To check if
        // stage filtering really works we need to have a last stage query specification that
        // picks only a subset of the "leaf" nodes at the bottom layer, so only a subset of the
        // parents are left after pruning.
        // We first issue a query to find the documents at the leafs, then we choose only a few of them
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.options.add(QueryOption.EXPAND_CONTENT);
        q.query.setTermPropertyName(QueryValidationServiceState.FIELD_NAME_LONG_VALUE)
                .setNumericRange(
                        NumericRange.createLongRange((long) stageCount - 1, (long) stageCount - 1,
                                true, true));


        this.host.waitFor("query result mismatch", () -> {
            QueryTask task = QueryTask.create(q);
            this.host.createQueryTaskService(this.queryFactoryUri,
                    task, false, true, task, null);
            Object doc = task.results.documents.values().iterator().next();
            QueryValidationServiceState st = Utils.fromJson(doc, QueryValidationServiceState.class);

            // augment last stage query, in the graph, to restrict results to this specific document
            Query specificDocClause = Query.Builder.create().addFieldClause(
                    QueryValidationServiceState.FIELD_NAME_ID, st.id).build();
            initialState.stages.get(initialState.stages.size() - 1).querySpec.query
                    .addBooleanClause(specificDocClause);

            GraphQueryTask finalState = this.createTask(initialState);
            logGraphQueryThroughput(finalState);

            // since the last stage contains a single result, and each parent points to N children,
            // with no two parents pointing to the same children, filtering should have
            // pruned the results in stages 1 and 0 to just a single result each.
            for (int i = 0; i < finalState.stages.size(); i++) {
                QueryTask stage = finalState.stages.get(i);
                if (1 != (long) stage.results.documentCount) {
                    return false;
                }
                if (1 != stage.results.documentLinks.size()) {
                    return false;
                }
                if (1 != stage.results.selectedLinksPerDocument.size()) {
                    return false;
                }

                // the last stage might have multiple selected links since stage filtering
                // does not prune the last stage
                if (i < (finalState.stages.size() - 1)
                        && (1 != stage.results.selectedLinks.size())) {
                    return false;
                }
            }

            return true;
        });
    }

    private GraphQueryTask createAndVerifyTreeGraphDirect(int stageCount, int[] resultCounts)
            throws Throwable {
        GraphQueryTask[] finalState = new GraphQueryTask[1];
        this.host.waitFor("query result mismatch", () -> {
            finalState[0] = createTreeGraphTask(stageCount, true);
            logGraphQueryThroughput(finalState[0]);
            return validateNStageResult(finalState[0], true, resultCounts);
        });
        return finalState[0];
    }

    private void createAndVerifyTreeGraphWithZeroStageResults(int stageCount,
            QueryTask finishedFirstStage, int[] resultCounts) throws Throwable {
        this.host.waitFor("query result mismatch", () -> {
            GraphQueryTask initialState = createTreeGraphTask(stageCount,
                    finishedFirstStage, false);
            GraphQueryTask finalState = waitForTask(initialState);
            logGraphQueryThroughput(finalState);
            return validateNStageResult(finalState, true, resultCounts);
        });
    }

    private void createAndVerifyDirectTreeGraphZeroStageResults(int stageCount, int[] resultCounts,
            QueryTask finishedFirstStage) throws Throwable {
        this.host.waitFor("query result mismatch", () -> {
            GraphQueryTask finalState = createTreeGraphTask(stageCount, finishedFirstStage, true);
            logGraphQueryThroughput(finalState);
            return validateNStageResult(finalState, true, resultCounts);
        });
    }

    private void createAndVerifyDirectTreeGraphWithZeroStagePaginatedResults(int stageCount,
            QueryTask stageWithResults) throws Throwable {

        this.host.waitFor("query result mismatch", () -> {
            URI firstStageTaskUri = this.host.createQueryTaskService(this.queryFactoryUri,
                    stageWithResults,
                    true, false, stageWithResults, null);

            QueryTask finishedFirstStage = this.host.waitForQueryTask(firstStageTaskUri,
                    TaskStage.FINISHED);
            GraphQueryTask finalState = createTreeGraphTask(stageCount, finishedFirstStage,
                    true);

            if (this.isFailureExpected) {
                assertEquals(null, finalState);
                return true;
            }

            logGraphQueryThroughput(finalState);

            int pageLimit = stageWithResults.querySpec.resultLimit;
            int[] pagedResultCounts = {
                    pageLimit,
                    pageLimit * this.linkCount,
                    pageLimit * this.linkCount * this.linkCount
            };

            return validateNStageResult(finalState, true, pagedResultCounts);
        });
    }

    private boolean validateNStageResult(GraphQueryTask finalState, int... expectedCounts) {
        return validateNStageResult(finalState, false, expectedCounts);
    }

    private boolean validateNStageResult(GraphQueryTask finalState, boolean isRecursive,
            int... expectedCounts) {
        for (int i = 0; i < expectedCounts.length; i++) {
            int expectedCount = expectedCounts == null ? this.serviceCount : expectedCounts[i];
            ServiceDocumentQueryResult stageOneResults = finalState.stages.get(i).results;
            boolean isFinalStage = i == expectedCounts.length - 1;
            if (!validateStageResults(stageOneResults, i, expectedCount, isRecursive, isFinalStage)) {
                return false;
            }
        }
        return true;
    }

    private boolean validateStageResults(ServiceDocumentQueryResult stage,
            int expectedResultCount, boolean isFinalStage) {
        return validateStageResults(stage, 0, expectedResultCount, false, isFinalStage);
    }

    private boolean validateStageResults(ServiceDocumentQueryResult stageResults,
            int stageIndex,
            int expectedResultCount, boolean isRecursive, boolean isFinalStage) {
        if (stageResults == null) {
            return false;
        }
        assertTrue(stageResults.queryTimeMicros > 0);
        if (stageResults.documentCount != expectedResultCount) {
            return false;
        }
        if (stageResults.documentLinks.size() != expectedResultCount) {
            return false;
        }
        if (!isFinalStage && stageResults.selectedLinks == null) {
            if (expectedResultCount > 0) {
                throw new IllegalStateException("null selectedLinks");
            }
        } else if (!isFinalStage) {
            int expectedLinkCount = expectedResultCount;
            if (isRecursive) {
                expectedLinkCount *= this.linkCount;
            }
            if (stageResults.selectedLinks.size() != expectedLinkCount) {
                return false;
            }
        }
        return true;
    }

    private void validateEmptyResultTask(GraphQueryTask finalState) {
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
        return createTreeGraphTask(stageCount, EnumSet.noneOf(GraphQueryOption.class), initialStage,
                isDirect);
    }

    private GraphQueryTask createTreeGraphTask(int stageCount,
            EnumSet<GraphQueryOption> options,
            QueryTask initialStage,
            boolean isDirect) throws Throwable {
        GraphQueryTask initialState = createGraphTaskState(stageCount, options, initialStage);
        initialState.taskInfo = new TaskState();
        initialState.taskInfo.isDirect = isDirect;
        initialState = createTask(initialState);
        return initialState;
    }

    private GraphQueryTask createGraphTaskState(int stageCount, EnumSet<GraphQueryOption> options,
            QueryTask initialStage) {
        GraphQueryTask.Builder builder = GraphQueryTask.Builder.create(stageCount);
        options.forEach((op) -> builder.addOption(op));

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

        Operation post = Operation.createPost(this.graphQueryFactoryUri);
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
        URI taskUri = this.host.getPeerServiceUri(initialState.documentSelfLink);
        if (taskUri == null) {
            taskUri = UriUtils.buildUri(this.host, initialState.documentSelfLink);
        }
        GraphQueryTask t = this.host.waitForFinishedTask(GraphQueryTask.class, taskUri);
        this.taskCompletionTimeMicros = Utils.getNowMicrosUtc();
        TestContext ctx = testCreate(1);
        Operation get = Operation.createGet(taskUri)
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
            if (stage.results == null) {
                continue;
            }
            if (stage.results.selectedLinks != null) {
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
                }, this.exampleFactoryUri);

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
                }, this.exampleFactoryUri);
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
                nextLayerLinks.add(UriUtils.buildUriPath(
                        GraphQueryValidationTestService.FACTORY_LINK,
                        "layer-" + (layer + 1) + "-" + UUID.randomUUID().toString()));
            }

            this.host.log(
                    "Graph vertex counts, next: %d, previous:%d, layer:%d",
                    nextLayerLinks.size(),
                    previousLayerLinks.size(),
                    layer);
            if (previousLayerLinks.isEmpty()) {
                for (int i = 0; i < this.serviceCount; i++) {
                    previousLayerLinks.add(UriUtils.buildUriPath(
                            GraphQueryValidationTestService.FACTORY_LINK,
                            "layer-" + layer + "-" + UUID.randomUUID().toString()));
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
            initState.id = UUID.randomUUID().toString();
            initState.longValue = (long) layer;
            initState.serviceLink = exampleStateIt.next().documentSelfLink;
            initState.serviceLinks = new ArrayList<>();
            initState.documentSelfLink = link;
            for (int l = 0; l < this.linkCount; l++) {
                initState.serviceLinks.add(UriUtils.normalizeUriPath(nextLayerLinkIt.next()));
            }
            Operation post = Operation.createPost(this.queryTargetFactoryUri)
                    .setBody(initState)
                    .setCompletion(ctx.getCompletion());
            this.host.send(post);
        }
        testWait(ctx);
    }

}
