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

import java.util.List;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;

/**
 * Implements a multistage pipeline, modeled as a task, that supports graph and value join queries
 */
public class GraphQueryTaskService extends TaskService<GraphQueryTaskService.GraphQueryTask> {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE_GRAPH_QUERIES;

    public static FactoryService createFactory() {
        return FactoryService.create(GraphQueryTaskService.class);
    }

    public static class QueryStageSpec {
        public QuerySpecification spec;
        public String edgePropertyName;
    }

    public static class GraphQueryTask extends TaskService.TaskServiceState {
        /**
         * Specifies a sequence of query specifications that select the graph nodes
         * serving as the origin of the graph search, at a given depth/stage in the query.
         * The query returns zero or more documents serving as the origin nodes for the
         * next stage of the graph traversal.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public List<QueryStageSpec> traversalSpecs;

        /**
         * Links to the query task service instances with results for each query stage
         */
        @PropertyOptions(usage = {
                PropertyUsageOption.LINK,
                PropertyUsageOption.SERVICE_USE })
        public List<String> resultLinks;

        @UsageOption(option = PropertyUsageOption.SINGLE_ASSIGNMENT)
        public int depthLimit;
        @UsageOption(option = PropertyUsageOption.SERVICE_USE)
        public int currentDepth;
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
            handleSubstage(currentState, body);
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

    private void handleSubstage(GraphQueryTask currentState, GraphQueryTask body) {

        // Determine if query is complete. It has two termination conditions:
        // 1) we have reached depth limit
        // 2) there are no results for the current depth

        if (body.currentDepth >= body.depthLimit) {
            // traversal complete
            body.taskInfo.stage = TaskStage.FINISHED;
            sendSelfPatch(body);
            return;
        }

        // Create a query task for our current query depth. If we have less query specs than
        // the depth limit, we re-use the query specification at the end of the traversal list
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
