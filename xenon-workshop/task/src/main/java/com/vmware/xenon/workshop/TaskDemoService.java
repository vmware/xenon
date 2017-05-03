/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.workshop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.TaskFactoryService;
import com.vmware.xenon.services.common.TaskService;

public class TaskDemoService extends TaskService<TaskDemoService.DemoTaskState> {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/demo-tasks";

    public static FactoryService createFactory() {
        return TaskFactoryService.create(TaskDemoService.class);
    }

    public enum SubStage {
        DO_STUFF
    }

    public static class DemoTaskState extends TaskService.TaskServiceState {

        /**
         * Sub-stage of the current task.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public SubStage subStage;

        // Additional task state goes here
    }

    public TaskDemoService() {
        super(DemoTaskState.class);
    }

    /**
     * This method is invoked when a factory POST causes a new task to be created.
     * @param startOp Supplies the POST {@link Operation} which has caused this task to be created.
     */
    @Override
    public void handleStart(Operation startOp) {
        DemoTaskState task = validateStartPost(startOp);
        if (task == null) {
            return;
        }

        initializeState(task, startOp);

        startOp.setBody(task)
                .setStatusCode(Operation.STATUS_CODE_ACCEPTED)
                .complete();

        sendSelfPatch(task, TaskState.TaskStage.STARTED, this::initializePatchState);
    }

    /**
     * This method validates the initial state of a task.
     * @param startOp Supplies the POST {@link Operation} which has caused this task to be created.
     * @return A {@link DemoTaskState} initialized from the operation, or null on failure.
     */
    @Override
    protected DemoTaskState validateStartPost(Operation startOp) {
        DemoTaskState taskState = super.validateStartPost(startOp);
        if (taskState == null) {
            return null;
        }

        if (!ServiceHost.isServiceCreate(startOp)) {
            return taskState;
        }

        if (taskState.taskInfo != null && taskState.taskInfo.stage != null) {
            startOp.fail(new IllegalArgumentException("taskInfo.stage may not be specified"));
            return null;
        }

        if (taskState.subStage != null) {
            startOp.fail(new IllegalArgumentException("subStage may not be specified"));
            return null;
        }

        // Additional input validation should go here.

        return taskState;
    }

    /**
     * This method initializes a {@link DemoTaskState} from the body of a valid POST operation.
     * @param taskState Supplies the task state to be initialized.
     * @param startOp Supplies the POST {@link Operation} which has caused this task to be created.
     */
    @Override
    protected void initializeState(DemoTaskState taskState, Operation startOp) {
        super.initializeState(taskState, startOp);
        taskState.taskInfo.stage = TaskState.TaskStage.CREATED;
    }

    private void initializePatchState(DemoTaskState taskState) {
        taskState.taskInfo.stage = TaskState.TaskStage.STARTED;
        taskState.subStage = SubStage.DO_STUFF;
    }

    @Override
    public void handlePatch(Operation patchOp) {
        DemoTaskState currentState = getState(patchOp);
        DemoTaskState patchBody = patchOp.getBody(DemoTaskState.class);

        if (!validateTransition(patchOp, currentState, patchBody)) {
            return;
        }

        super.updateState(currentState, patchBody);
        patchOp.complete();

        switch (currentState.taskInfo.stage) {
        case CREATED:
            // Should never happen; validateTransition will not allow a transition to CREATED.
            break;
        case STARTED:
            processSubStage(currentState);
            break;
        case FINISHED:
            logInfo("Task finished successfully");
            break;
        case FAILED:
            logWarning("Task failed: %s", currentState.failureMessage);
            break;
        case CANCELLED:
            logInfo("Task cancelled: not implemented, ignoring");
            break;
        default:
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean validateTransition(Operation patchOp, DemoTaskState currentState,
            DemoTaskState patchBody) {
        if (!super.validateTransition(patchOp, currentState, patchBody)) {
            return false;
        }

        if (patchBody.taskInfo.stage == TaskState.TaskStage.STARTED) {
            // A sub-stage must be specified when in STARTED stage
            if (patchBody.subStage == null) {
                patchOp.fail(new IllegalArgumentException("subStage is required"));
                return false;
            }

            // A patch cannot transition the task to an earlier sub-stage
            if (currentState.taskInfo.stage == TaskState.TaskStage.STARTED
                    && patchBody.subStage.ordinal() < currentState.subStage.ordinal()) {
                patchOp.fail(new IllegalArgumentException("Invalid sub-stage"));
                return false;
            }
        }

        return true;
    }

    private void processSubStage(DemoTaskState currentState) {
        switch (currentState.subStage) {
        case DO_STUFF:
            processDoStuffStage(currentState);
            break;
        default:
            throw new IllegalStateException();
        }
    }

    private void processDoStuffStage(DemoTaskState currentState) {

        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(ExampleService.ExampleServiceState.class)
                .build();

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(query)
                .build();

        Operation queryPostOp = Operation
                .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBody(queryTask)
                .setCompletion((Operation op, Throwable ex) -> {
                    if (ex != null) {
                        failTask(currentState, ex);
                        return;
                    }

                    QueryTask queryResult = op.getBody(QueryTask.class);
                    processQueryResults(currentState, queryResult);
                });

        sendRequest(queryPostOp);
    }

    private void processQueryResults(DemoTaskState currentState, QueryTask queryResult) {

        // If there are no query results, then there's nothing to do.
        if (queryResult.results.documentLinks.isEmpty()) {
            logInfo("Found no results of type ExampleService");
            sendSelfFinishedPatch(currentState);
            return;
        }

        // Create one delete Operation per document to delete
        List<Operation> deleteOps = new ArrayList<>();
        for (String documentLink : queryResult.results.documentLinks) {
            Operation deleteOp = Operation.createDelete(this, documentLink);
            deleteOps.add(deleteOp);
        }

        // Send the delete operations using an OperationJoin
        OperationJoin
                .create(deleteOps)
                .setCompletion((Map<Long, Operation> ops, Map<Long, Throwable> exs) -> {
                    if (exs != null && !exs.isEmpty()) {
                        failTask(currentState, exs.values());
                        return;
                    }

                    logInfo("Deleted %d ExampleService instances", ops.size());
                    sendSelfFinishedPatch(currentState);
                })
                .sendWith(this);
    }

    private void failTask(DemoTaskState currentState, Collection<Throwable> exs) {
        failTask(currentState, exs.iterator().next());
    }

    private void failTask(DemoTaskState currentState, Throwable e) {
        sendSelfFailurePatch(currentState, Utils.toString(e));
    }
}
