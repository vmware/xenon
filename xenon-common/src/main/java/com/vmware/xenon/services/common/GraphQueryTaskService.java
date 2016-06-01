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
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;

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

        for (QueryTask stage : task.stages) {
            // basic validation of query specifications, per stage. The query task created
            // during the state machine operation will do deeper validation and the graph query
            // will self patch to failure if the stage query is invalid.
            if (stage.querySpec == null || stage.querySpec.query == null) {
                taskOperation.fail(new IllegalArgumentException(
                        "Stage query specification is invalid: " + Utils.toJson(stage)));
                return null;
            }
            stage.results = null;
        }

        return task;
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

        // a graph query can be reset to a previous stage, to process the next page worth of results
        if (patchBody.currentDepth < currentState.currentDepth) {
            if (patchBody.currentDepth >= currentState.stages.size() - 1) {
                patch.fail(new IllegalStateException(
                        "new currentDepth is must be a valid stage index, other than final stage"
                                + currentState.currentDepth));
                return false;
            }
            QueryTask stage = currentState.stages.get(patchBody.currentDepth);
            if (stage.results == null || stage.results.nextPageLink == null) {
                patch.fail(new IllegalStateException(
                        "currentDepth points to stage with no results, or results with no pages: "
                                + Utils.toJsonHtml(stage)));
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
     *
     * If the task is in process, it will call this method twice, per depth:
     * 1) When a query for the current depth completes, it will self patch to STARTED
     * but increment the depth. This will call this method with lastResults == null, but
     * currentDepth > 0
     * 2) We need to fetch the results from the previous stage, before we proceed to the
     * next, so we call {@link GraphQueryTaskService#fetchLastStageResults(GraphQueryTask)}
     * which will then call this method again, but with lastResults != null
     *
     * The termination conditions are applied only when lastResults != null
     */
    private void startOrContinueGraphQuery(GraphQueryTask currentState) {
        int previousStageIndex = Math.max(currentState.currentDepth - 1, 0);
        ServiceDocumentQueryResult lastResults = currentState.stages.get(previousStageIndex).results;

        if (lastResults != null) {
            // Determine if query is complete. It has two termination conditions:
            // 1) we have reached depth limit
            // 2) there are no results for the current depth
            // 3) there are no selected links for the current depth

            if (currentState.currentDepth > currentState.depthLimit - 1) {
                sendSelfPatch(currentState, TaskStage.FINISHED, null);
                return;
            }

            if (lastResults.documentCount == 0) {
                sendSelfPatch(currentState, TaskStage.FINISHED, null);
                return;
            }

            if (lastResults.selectedLinks == null || lastResults.selectedLinks.isEmpty()) {
                if (currentState.currentDepth < currentState.depthLimit - 1) {
                    // this is either a client error, the query specified the wrong field for the link terms, or
                    // the documents had null link values. We can not proceed, since there are no edges to
                    // traverse.
                    sendSelfPatch(currentState, TaskStage.FINISHED, null);
                    return;
                }
            }
        }

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

        // enable connection sharing (HTTP/2) since we want to use a direct task, but avoid
        // holding up a connection
        Operation createQueryOp = Operation.createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBodyNoCloning(task)
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    handleQueryStageCompletion(currentState, o, e);
                });
        sendRequest(createQueryOp);
    }

    private void handleQueryStageCompletion(GraphQueryTask currentState, Operation o, Throwable e) {
        if (e != null) {
            sendSelfFailurePatch(currentState, e.toString());
            return;
        }
        QueryTask response = o.getBody(QueryTask.class);
        // associate the query result for the current depth. If the query is paginated, we store
        // the result for the next page we need to fetch
        if (response.querySpec.resultLimit != null
                && response.results.nextPageLink != null) {
            currentState.resultLinks.set(currentState.resultLinks.size() - 1,
                    response.results.nextPageLink);
        } else {
            currentState.resultLinks.add(response.documentSelfLink);
        }

        int traversalSpecIndex = Math.min(currentState.stages.size() - 1,
                currentState.currentDepth);
        currentState.stages.get(traversalSpecIndex).results = response.results;
        if (response.results.nextPageLink == null) {
            // We increase the depth, moving to the next stage only when we run out of pages to
            // process in the current stage. For non paginated queries, we move forward on every
            // query completion
            currentState.currentDepth++;
        }

        // Self PATCH, staying in STARTED stage. Termination conditions are checked
        // in the main state machine method
        sendSelfPatch(currentState, TaskStage.STARTED, null);
    }

    private void scopeNextStageQueryToSelectedLinks(ServiceDocumentQueryResult lastResults,
            QueryTask task) {
        if (lastResults == null) {
            // we must be in initial stage, otherwise termination checks would have kicked in
            return;
        }
        // augment query specification for current depth, using the selected links from the last
        // stage results. This restricts the query scope to only documents that are "linked" from
        // from the current stage to the next, effectively guiding our search of the index, across
        // the graph edges (links) specified in each traversal specification
        Query selfLinkClause = new Query();
        selfLinkClause.setOccurance(Occurance.MUST_OCCUR);

        for (String link : lastResults.selectedLinks) {
            Query clause = new Query()
                    .setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK)
                    .setTermMatchValue(link)
                    .setTermMatchType(MatchType.TERM)
                    .setOccurance(Occurance.SHOULD_OCCUR);
            selfLinkClause.addBooleanClause(clause);
        }
        task.querySpec.query.addBooleanClause(selfLinkClause);
    }

    @Override
    protected void initializeState(GraphQueryTask task, Operation taskOperation) {
        task.currentDepth = 0;
        task.resultLinks = new ArrayList<>();

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
        }
        super.initializeState(task, taskOperation);
    }
}
