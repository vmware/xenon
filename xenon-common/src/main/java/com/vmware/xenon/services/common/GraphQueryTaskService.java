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

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;

/**
 * Implements a multistage pipeline, modeled as a task, that supports graph and value join queries
 */
public class GraphQueryTaskService extends TaskService<GraphQueryTask> {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE_GRAPH_QUERIES;

    public static FactoryService createFactory() {
        return FactoryService.create(GraphQueryTaskService.class);
    }

    public static class QueryStageSpec {
        public QuerySpecification spec;
        public String edgePropertyName;
    }

    public GraphQueryTaskService() {
        super(GraphQueryTask.class);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    public void handlePatch(Operation patch) {
        GraphQueryTask currentState = getState(patch);
        GraphQueryTask body = getBody(patch);

        // TODO validate state transition
        updateState(currentState, body);
        patch.complete();

        switch (body.taskInfo.stage) {
        case CREATED:
            // Won't happen: validateTransition reports error
            break;
        case STARTED:
            startOrContinueGraphQuery(currentState, body);
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

    /**
     * Issues a query for the current depth in the graph search, and processes results. If the
     * query specification for the current depth is paginated, we process all pages first.
     * When the query results have been processed, we self patch to increment the depth, OR
     * if no results are found in the current state, we self PATCH to FINISHED
     */
    private void startOrContinueGraphQuery(GraphQueryTask currentState, GraphQueryTask body) {

        // Determine if query is complete. It has two termination conditions:
        // 1) we have reached depth limit
        // 2) there are no results for the current depth

        if (body.currentDepth >= body.depthLimit) {
            // traversal complete
            body.taskInfo.stage = TaskStage.FINISHED;
            sendSelfPatch(body);
            return;
        }

        // update current search depth, which represents number of hops (edges) from source nodes
        currentState.currentDepth = body.currentDepth;
        // Create a query task for our current query depth. If we have less query specs than
        // the depth limit, we re-use the query specification at the end of the traversal list
        QueryTask task = QueryTask.Builder.createDirectTask().build();
        task.querySpec = currentState.traversalSpecs.get(currentState.currentDepth).spec;

        Operation createQueryOp = Operation.createPost(this, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                .setBodyNoCloning(task)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        sendSelfFailurePatch(currentState, e.getMessage());
                        return;
                    }
                    processQueryResults(currentState, o.getBody(QueryTask.class), null);
                });
        sendRequest(createQueryOp);
    }

    private void processQueryResults(GraphQueryTask currentState, QueryTask response,
            String nextPage) {
        currentState.resultLinks.add(response.documentSelfLink);

    }

    protected GraphQueryTask validateStartPost(Operation taskOperation) {
        GraphQueryTask task = super.validateStartPost(taskOperation);
        if (task == null) {
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

    protected void initializeState(GraphQueryTask task, Operation taskOperation) {
        task.currentDepth = 0;

        // parent method will issue a self PATCH to initiate the state machine
        super.initializeState(task, taskOperation);
    }
}
