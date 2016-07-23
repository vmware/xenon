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

import java.util.ArrayList;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

/**
 * Implements a multistage query pipeline, modeled as a task, that supports graph traversal queries
 */
public class GraphQueryTaskService extends TaskService<GraphQueryTask> {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE_GRAPH_QUERIES;

    public GraphQueryTaskService() {
        super(GraphQueryTask.class);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation post) {
        GraphQueryTask initialState = validateStartPost(post);
        if (initialState == null) {
            return;
        }

        initializeState(initialState, post);
        initialState.taskInfo.stage = TaskStage.CREATED;
        post.setBody(initialState)
                .setStatusCode(Operation.STATUS_CODE_ACCEPTED)
                .complete();

        // self patch to start state machine
        sendSelfPatch(initialState, TaskStage.STARTED, null);
    }

    @Override
    protected GraphQueryTask validateStartPost(Operation taskOperation) {
        GraphQueryTask task = super.validateStartPost(taskOperation);
        if (task == null) {
            return null;
        }

        if (!ServiceHost.isServiceCreate(taskOperation)) {
            return task;
        }

        if (task.currentDepth > 0) {
            taskOperation.fail(
                    new IllegalArgumentException("Do not specify currentDepth: internal use only"));
            return null;
        }
        if (task.depthLimit < 2) {
            taskOperation.fail(
                    new IllegalArgumentException(
                            "depthLimit must be a positive integer greater than one"));
            return null;
        }
        if (task.stages == null || task.stages.isEmpty()) {
            taskOperation.fail(new IllegalArgumentException(
                    "At least one stage is required"));
            return null;
        }

        if (task.stages.size() != task.depthLimit) {
            taskOperation.fail(new IllegalArgumentException(
                    "Number of stages must match depthLimit"));
            return null;
        }

        for (int i = 0; i < task.stages.size(); i++) {
            QueryTask stage = task.stages.get(i);
            // basic validation of query specifications, per stage. The query task created
            // during the state machine operation will do deeper validation and the graph query
            // will self patch to failure if the stage query is invalid.
            if (stage.querySpec == null || stage.querySpec.query == null) {
                taskOperation.fail(new IllegalArgumentException(
                        "Stage query specification is invalid: " + Utils.toJson(stage)));
                return null;
            }

            if (i != 0 || stage.results == null) {
                if (stage.querySpec.resultLimit != null) {
                    taskOperation.fail(new IllegalArgumentException(
                            "Stage query specification resultLimit must be null: "
                                    + Utils.toJson(stage)));
                    return null;
                }
                continue;
            }

            if (stage.results.nextPageLink == null
                    && !hasInlineResults(stage.results)) {
                taskOperation.fail(new IllegalArgumentException(
                        "First stage has results instance but no actual results: "
                                + Utils.toJson(stage)));
                return null;
            }

            if (stage.results.nextPageLink != null
                    && !stage.querySpec.options.contains(QueryOption.SELECT_LINKS)) {
                taskOperation.fail(new IllegalArgumentException(
                        "First stage has paginated results but does not specify option "
                                + QueryOption.SELECT_LINKS + " :" + Utils.toJson(stage)));
                return null;
            }

            if (stage.documentSelfLink == null) {
                taskOperation.fail(new IllegalArgumentException(
                        "First stage with results must have valid self link: "
                                + Utils.toJson(stage)));
                return null;
            }
        }

        return task;
    }

    @Override
    protected void initializeState(GraphQueryTask task, Operation taskOperation) {
        task.currentDepth = 0;
        task.resultLinks.clear();

        for (int i = 0; i < task.stages.size(); i++) {
            QueryTask stageQueryTask = task.stages.get(i);
            stageQueryTask.taskInfo = new TaskState();
            // we use direct tasks, and HTTP/2 for each stage query, keeping things simple but
            // without consuming connections while task is pending
            stageQueryTask.taskInfo.isDirect = true;

            if (i < task.stages.size() - 1) {
                // stages other than the last one, must set select links, since we need to guide
                // the query along the edges of the document graph.
                stageQueryTask.querySpec.options.add(QueryOption.SELECT_LINKS);
            }

            if (task.tenantLinks != null && stageQueryTask.tenantLinks == null) {
                stageQueryTask.tenantLinks = new ArrayList<>(task.tenantLinks);
            }

            if (i == 0) {
                // a client can initiate a graph query by supplying a initial stage with results
                if (hasInlineResults(stageQueryTask.results)) {
                    logInfo("First stage has %d (page:%s) results, skipping query, moving to next stage",
                            stageQueryTask.results.documentCount,
                            stageQueryTask.results.nextPageLink);
                    task.resultLinks.add(stageQueryTask.documentSelfLink);
                    task.currentDepth = 1;
                } else if (stageQueryTask.results != null) {
                    // the results object is populated but there are no actual results, it just means we
                    // need to fetch them as part of stage zero execution, by simply doing a GET
                    logInfo("First stage result link %s, bypassing query execution will fetch results",
                            stageQueryTask.results.nextPageLink);
                }
            } else {
                stageQueryTask.results = null;
            }
        }
        super.initializeState(task, taskOperation);
    }

    public void handlePatch(Operation patch) {
        GraphQueryTask currentState = getState(patch);
        GraphQueryTask body = getBody(patch);

        if (!validateTransition(patch, currentState, body)) {
            return;
        }

        updateState(currentState, body);
        patch.complete();

        switch (body.taskInfo.stage) {
        case STARTED:
            startOrContinueGraphQuery(currentState);
            break;
        case CANCELLED:
            logInfo("Task canceled: not implemented, ignoring");
            break;
        case FINISHED:
            logFine("Task finished successfully");
            break;
        case FAILED:
            logWarning("Task failed: %s", (body.failureMessage == null ? "No reason given"
                    : body.failureMessage));
            break;
        default:
            break;
        }
    }

    @Override
    protected void updateState(GraphQueryTask task, GraphQueryTask patchBody) {
        super.updateState(task, patchBody);
        // we replace the stages and result links since they are lists and their items have
        // modifications. The merge utility is not sophisticated enough to merge lists
        task.stages = patchBody.stages;
        task.resultLinks = patchBody.resultLinks;
    }

    @Override
    protected boolean validateTransition(Operation patch, GraphQueryTask currentState,
            GraphQueryTask patchBody) {
        if (patchBody.taskInfo == null) {
            patch.fail(new IllegalArgumentException("Missing taskInfo"));
            return false;
        }
        if (patchBody.taskInfo.stage == null) {
            patch.fail(new IllegalArgumentException("Missing stage"));
            return false;
        }
        if (patchBody.taskInfo.stage == TaskState.TaskStage.CREATED) {
            patch.fail(new IllegalArgumentException("Did not expect to receive CREATED stage"));
            return false;
        }
        if (patchBody.taskInfo.stage == TaskStage.STARTED) {
            if (patchBody.currentDepth > currentState.stages.size() - 1) {
                patch.fail(new IllegalStateException(
                        "new currentDepth is must be a valid stage index: "
                                + currentState.currentDepth));
                return false;
            }
        }
        if (patchBody.currentDepth < currentState.currentDepth) {
            patch.fail(new IllegalStateException(
                    "new currentDepth is less or equal to existing depth:"
                            + currentState.currentDepth));
            return false;
        }

        if (patchBody.stages.size() != currentState.stages.size()) {
            patch.fail(new IllegalStateException(
                    "Query stages can not be modified after task is created"));
            return false;
        }

        return true;
    }

    /**
     * Main state machine for the multiple stage graph traversal.
     *
     * If the task is just starting, the currentDepth will be zero. In this case
     * we will issue the first query in the  {@link GraphQueryTask#stages}.
     */
    private void startOrContinueGraphQuery(GraphQueryTask currentState) {
        int previousStageIndex = Math.max(currentState.currentDepth - 1, 0);
        ServiceDocumentQueryResult lastResults = currentState.stages.get(previousStageIndex).results;

        // Use a query task for our current query depth. If we have less query specifications
        // than the depth limit, we re-use the query specification at the end of the traversal list
        int traversalSpecIndex = Math.min(currentState.stages.size() - 1,
                currentState.currentDepth);
        // The traversal query should contain linkTerms which tell us which edges / links we need to
        // traverse. If it does not, then the query task validation will fail, and the graph query
        // will self patch to failed.
        QueryTask task = currentState.stages.get(traversalSpecIndex);
        task.documentExpirationTimeMicros = currentState.documentExpirationTimeMicros;

        scopeNextStageQueryToSelectedLinks(lastResults, task);

        Operation getResultsOrStartQueryOp = null;

        if (currentState.currentDepth == 0 && lastResults != null) {
            // We allow a first stage to come with results, either in-line, or through a link.
            // If a result instance with empty document links but a page link was supplied,
            // we stayed at currentDepth == 0 and we need to fetch the results, without executing
            // the query
            logInfo("Fetching initial stage results from %s", lastResults.nextPageLink);
            getResultsOrStartQueryOp = Operation.createGet(this, lastResults.nextPageLink)
                    .setCompletion((o, e) -> {
                        handleQueryPageGetCompletion(currentState, o, e);
                    });
        } else {
            // we need to execute a query for this stage
            // enable connection sharing (HTTP/2) since we want to use a direct task, but avoid
            // holding up a connection
            getResultsOrStartQueryOp = Operation.createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                    .setBodyNoCloning(task)
                    .setConnectionSharing(true)
                    .setCompletion((o, e) -> {
                        handleQueryStageCompletion(currentState, o, e);
                    });
        }
        sendRequest(getResultsOrStartQueryOp);
    }

    private void scopeNextStageQueryToSelectedLinks(ServiceDocumentQueryResult lastResults,
            QueryTask task) {
        if (!hasInlineResults(lastResults)) {
            return;
        }
        // Use query context white list, using the selected links, or documentLinks, from the last
        // stage results. This restricts the query scope to only documents that are "linked"
        // from the current stage to the next, effectively guiding our search of the index, across
        // the graph edges (links) specified in each traversal specification.
        // This is a performance optimization: the alternative would have been a massive boolean
        // clause with SHOULD_OCCUR child clauses for each link
        logFine("Setting whitelist to %d links", lastResults.selectedLinks.size());
        task.querySpec.context.documentLinkWhiteList = lastResults.selectedLinks;
    }

    private boolean checkAndPatchToFinished(GraphQueryTask currentState,
            ServiceDocumentQueryResult lastResults) {
        if (lastResults == null) {
            return false;
        }
        // Determine if query is complete. It has two termination conditions:
        // 1) we have reached depth limit
        // 2) there are no results for the current depth
        // 3) there are no selected links for the current depth

        if (currentState.currentDepth > currentState.depthLimit - 1) {
            sendSelfPatch(currentState, TaskStage.FINISHED, null);
            return true;
        }

        if (lastResults.documentCount == null || lastResults.documentCount == 0) {
            sendSelfPatch(currentState, TaskStage.FINISHED, null);
            return true;
        }

        if (lastResults.selectedLinks == null || lastResults.selectedLinks.isEmpty()) {
            if (currentState.currentDepth < currentState.depthLimit - 1) {
                // this is either a client error, the query specified the wrong field for the link terms, or
                // the documents had null link values. We can not proceed, since there are no edges to
                // traverse.
                sendSelfPatch(currentState, TaskStage.FINISHED, null);
                return true;
            }
        }
        return false;
    }

    private void handleQueryPageGetCompletion(GraphQueryTask currentState, Operation o,
            Throwable e) {
        if (e != null) {
            handleQueryStageCompletion(currentState, o, e);
            return;
        }
        QueryTask response = o.getBody(QueryTask.class);
        if (response.results == null) {
            handleQueryStageCompletion(currentState, o,
                    new IllegalStateException("No results found for page in first stage results"));
            return;
        }

        if (response.results.selectedLinks == null || response.results.selectedLinks.isEmpty()) {
            sendSelfFailurePatch(currentState,
                    "Paginated first stage response had no selected link results: "
                            + Utils.toJsonHtml(response));
            return;
        }
        handleQueryStageCompletion(currentState, o, null);
    }

    private void handleQueryStageCompletion(GraphQueryTask currentState, Operation o, Throwable e) {
        if (e != null) {
            sendSelfFailurePatch(currentState, e.toString());
            return;
        }
        QueryTask response = o.getBody(QueryTask.class);
        // associate the query result for the current depth
        currentState.resultLinks.add(response.documentSelfLink);

        int traversalSpecIndex = Math.min(currentState.stages.size() - 1,
                currentState.currentDepth);
        QueryTask stage = currentState.stages.get(traversalSpecIndex);
        stage.results = response.results;
        stage.documentSelfLink = response.documentSelfLink;
        stage.documentOwner = response.documentOwner;

        // We increment the depth, moving to the next stage
        currentState.currentDepth++;

        // check termination conditions and patch to FINISHED if appropriate
        if (checkAndPatchToFinished(currentState, response.results)) {
            return;
        }

        // Self PATCH, staying in STARTED stage. Termination conditions are checked
        // in the main state machine method
        sendSelfPatch(currentState, TaskStage.STARTED, null);
    }

    private static boolean hasInlineResults(ServiceDocumentQueryResult results) {
        return results != null && results.documentCount != null
                && results.documentCount > 0
                && results.selectedLinks != null
                && !results.selectedLinks.isEmpty();
    }
}
