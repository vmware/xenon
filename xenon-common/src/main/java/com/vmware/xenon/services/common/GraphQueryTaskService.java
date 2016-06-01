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

import java.util.HashSet;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

/**
 * Implements a multistage pipeline, modeled as a task, that supports graph and value join queries
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
            // validation failed
            return;
        }

        post.setBody(initialState).setStatusCode(Operation.STATUS_CODE_ACCEPTED).complete();
        sendSelfPatch(initialState, TaskStage.STARTED, null);
    }

    @Override
    protected GraphQueryTask validateStartPost(Operation taskOperation) {
        GraphQueryTask task = super.validateStartPost(taskOperation);
        if (task == null) {
            return null;
        }

        if (task.taskInfo.isDirect) {
            taskOperation.fail(new IllegalArgumentException(
                    "Direct tasks are not supported"));
            return null;
        }

        if (!ServiceHost.isServiceCreate(taskOperation)) {
            return task;
        }


        if (task.traversalSpecs == null || task.traversalSpecs.isEmpty()) {
            taskOperation.fail(new IllegalArgumentException(
                    "At least one traversalSpec is required"));
            return null;
        }

        // Apply validation only for the initial creation POST, not restart. Alternatively,
        // this code can exist in the handleCreate method
        if (task.currentDepth > 0) {
            taskOperation.fail(
                    new IllegalArgumentException("Do not specify currentDepth: internal use only"));
            return null;
        }
        if (task.depthLimit == 0) {
            taskOperation.fail(
                    new IllegalArgumentException("depthLimit must be a positive integer"));
            return null;
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
        case CREATED:
            // Won't happen: validateTransition reports error
            break;
        case STARTED:
            startOrContinueGraphQuery(currentState, null);
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
    protected boolean validateTransition(Operation patch, GraphQueryTask currentState,
            GraphQueryTask patchBody) {
        if (!super.validateTransition(patch, currentState, patchBody)) {
            return false;
        }

        return true;
    }

    /**
     * Main state machine for the multiple stage graph traversal.
     *
     * If the task is just starting, the currentDepth will be zero. In this case
     * we will issue the first query in the  {@link GraphQueryTask#traversalSpecs}.
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
    private void startOrContinueGraphQuery(GraphQueryTask currentState,
            ServiceDocumentQueryResult lastResults) {

        if (lastResults != null) {
            // Determine if query is complete. It has two termination conditions:
            // 1) we have reached depth limit
            // 2) there are no results for the current depth
            if (currentState.currentDepth >= currentState.depthLimit) {
                // traversal complete
                sendSelfPatch(currentState, TaskStage.FINISHED, null);
                return;
            }

            if (lastResults.selectedLinks.isEmpty()
                    && lastResults.documentLinks.isEmpty()
                    && lastResults.nextPageLink == null) {
                // the last query did not return results
                sendSelfPatch(currentState, TaskStage.FINISHED, null);
                return;
            }
        } else {
            if (currentState.currentDepth > 0) {
                // we need to fetch the actual results from the previous stage, then continue
                fetchLastStageResults(currentState);
                return;
            }
        }

        // Create a query task for our current query depth. If we have less query specifications
        // than the depth limit, we re-use the query specification at the end of the traversal list
        QueryTask task = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.SELECT_LINKS)
                .build();
        int traversalSpecIndex = Math.min(currentState.traversalSpecs.size() - 1,
                currentState.currentDepth);
        // The traversal query should contain linkTerms which tell us which edges / links we need to
        // traverse. If it does not, then the query task validation will fail, and the graph query
        // will self patch to failed.
        task.querySpec = currentState.traversalSpecs.get(traversalSpecIndex);
        task.documentExpirationTimeMicros = currentState.documentExpirationTimeMicros;

        // augment query specification for current depth, using the selected links from the last
        // stage results. This restricts the query scope to only documents that are "linked" from
        // from the current stage to the next, effectively guiding our search of the index, across
        // the graph edges (links) specified in each traversal specification
        if (lastResults != null) {
        }

        // enable connection sharing (HTTP/2) since we want to use a direct task, but avoid
        // holding up a connection
        Operation createQueryOp = Operation.createPost(this, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                .setBodyNoCloning(task)
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        sendSelfFailurePatch(currentState, e.toString());
                        return;
                    }
                    QueryTask response = o.getBody(QueryTask.class);
                    currentState.resultLinks.add(response.documentSelfLink);
                    currentState.currentDepth++;
                    // continue with next depth, termination conditions are checked in the start or continue
                    // method
                    sendSelfPatch(currentState, TaskStage.STARTED, null);
                });
        sendRequest(createQueryOp);
    }

    private void fetchLastStageResults(GraphQueryTask currentState) {
        // TODO Auto-generated method stub

    }

    protected void initializeState(GraphQueryTask task, Operation taskOperation) {
        task.currentDepth = 0;
        task.resultLinks = new HashSet<>();

        // parent method will issue a self PATCH to initiate the state machine
        super.initializeState(task, taskOperation);
    }
}
