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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.QueryTask.Query;

/**
 * This is an example task service. When a new task is posted, it will delete ALL
 * example services. It does this in two steps: querying for the example services
 * and deleting them.
 *
 * Note that this task service will only be authorized to query or delete example
 * services taht are accessible to the user who created the task.
 *
 * This example task service embodies the typical task workflow, which looks like:
 *
 * 1. Client does a POST to the task factory service to create a task service
 * 2. The task service will do some work, almost certainly asychronously. (This
 *    task service does queries and DELETEs to other service.)
 * 3. When the work is completed, the task service will update it's state by
 *    PATCHing itself.
 * 4. When the PATCH is received, if more work is to be done, this will be
 *    repeated as needed.
 *
 * You can think of this as proceeding through a state machine. All tasks have a
 * "stage" {@link TaskStage} that covers just the major parts of the lifecycle,
 * such as CREATED, STARTED, and FINISHED. We'll encode the steps that we do as
 * SubStages. For this task service (and most), those sub stages will be associated
 * with just the STARTED TaskStage.
 *
 */
public class ExampleTaskService extends StatefulService {

    /**
     * These substages are for tracking the stages unique to our task service. They are only
     * relevant to the STARTED TaskStage. If you create your own task service, these substages
     * will probably be where most of the work happens. See the description above.
     */
    public static enum SubStage {
        QUERY_EXAMPLES, DELETE_EXAMPLES
    }

    /** Time in seconds for the task to live */
    private static long DEFAULT_TASK_LIFETIME = 60;

    public static class ExampleTaskServiceState extends ServiceDocument {

        /**
         * Time in seconds before the task expires
         *
         * Technically, this isn't needed: clients can just set documentExpirationTimeMicros.
         * However, this makes tutorials easier: command-line clients can just set this to
         * a constant instead of calculating a future time
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long taskLifetime;

        /**
         * This field shouldn't be manipulated by clients, but can be examined to see the progress
         * of the task
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public TaskState taskInfo;

        /**
         * If taskInfo.stage == FAILED, this message will say why
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String failureMessage;

        /**
         * The current substage. See {@link SubStage}
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public SubStage subStage;

        /**
         * The query we make to the Query Task service, and the result we
         * get back from it.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public QueryTask exampleQueryTask;
    }

    public ExampleTaskService() {
        super(ExampleTaskServiceState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    /**
     * This handles the initial POST that creates the task service.
     */
    @Override
    public void handleStart(Operation taskOperation) {
        ExampleTaskServiceState task = validateStartPost(taskOperation);
        if (task == null) {
            return;
        }
        taskOperation.complete();

        initializeState(task, taskOperation);
        sendSelfPatch(task);
    }

    /**
     * Ensure that the input task is valid.
     *
     * Technically we don't need to require a body since there are no parameters. However,
     * non-example tasks will normally have parameters, so this is an example of how they
     * could be validated.
     */
    private ExampleTaskServiceState validateStartPost(Operation taskOperation) {
        if (!taskOperation.hasBody()) {
            taskOperation.fail(new IllegalArgumentException("POST body is required"));
            return null;
        }

        ExampleTaskServiceState task = taskOperation.getBody(ExampleTaskServiceState.class);
        if (task.taskInfo != null) {
            taskOperation.fail(
                    new IllegalArgumentException("Do not specify taskBody: internal use only"));
            return null;
        }
        if (task.subStage != null) {
            taskOperation.fail(
                    new IllegalArgumentException("Do not specify subStage: internal use only"));
            return null;
        }
        if (task.exampleQueryTask != null) {
            taskOperation.fail(
                    new IllegalArgumentException("Do not specify taskBody: internal use only"));
            return null;
        }
        if (task.taskLifetime != null && task.taskLifetime <= 0) {
            taskOperation.fail(
                    new IllegalArgumentException("taskLifetime must be positive"));
            return null;
        }

        return task;
    }

    /**
     * Initialize the task
     *
     * We set it to be STARTED: we skip CREATED because we don't need the CREATED state
     * If your task does significant initialization, you may prefer to do it in the
     * CREATED state.
     */
    private void initializeState(ExampleTaskServiceState task, Operation taskOperation) {
        task.taskInfo = new TaskState();
        task.taskInfo.stage = TaskState.TaskStage.STARTED;
        task.subStage = SubStage.QUERY_EXAMPLES;

        if (task.taskLifetime != null) {
            task.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + TimeUnit.SECONDS.toMicros(task.taskLifetime);
        } else if (task.documentExpirationTimeMicros != 0) {
            task.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + TimeUnit.SECONDS.toMicros(DEFAULT_TASK_LIFETIME);
        }
        taskOperation.setBody(task);
    }

    /**
     * Handle PATCH
     *
     * All of the work happens through this task service patching itself. It does
     * an operation (for example, querying the example services), and when that completes
     * it updates the task state and progresses to the next step by doing a self PATCH.
     *
     */
    @Override
    public void handlePatch(Operation patch) {
        ExampleTaskServiceState currentTask = getState(patch);
        ExampleTaskServiceState patchBody = patch.getBody(ExampleTaskServiceState.class);

        if (!validateTransition(patch, currentTask, patchBody)) {
            return;
        }
        updateState(patch, currentTask, patchBody);
        patch.complete();

        switch (patchBody.taskInfo.stage) {
        case CREATED:
            // Won't happen: validateTransition reports error
            break;
        case STARTED:
            handleSubstage(patchBody);
            break;
        case CANCELLED:
            logInfo("Task canceled: not implemented, ignoring");
            break;
        case FINISHED:
            logInfo("Task finished successfully");
            break;
        case FAILED:
            logWarning("Task failed: %s", (patchBody.failureMessage == null ? "No reason given"
                    : patchBody.failureMessage));
            break;
        default:
            logWarning("Unexpected stage: %s", patchBody.taskInfo.stage);
            break;
        }
    }

    private void handleSubstage(ExampleTaskServiceState task) {
        switch (task.subStage) {
        case QUERY_EXAMPLES:
            handleQueryExamples(task);
            break;
        case DELETE_EXAMPLES:
            handleDeleteExamples(task);
            break;
        default:
            logWarning("Unexpected sub stage: %s", task.subStage);
            break;
        }
    }

    /**
     * Validate that the PATCH we got requests reasonanble changes to our state
     */
    private boolean validateTransition(Operation patch, ExampleTaskServiceState currentTask,
            ExampleTaskServiceState patchBody) {
        if (patchBody.taskInfo == null) {
            patch.fail(new IllegalArgumentException("Missing taskInfo"));
            return false;
        }
        if (patchBody.taskInfo.stage == null) {
            patch.fail(new IllegalArgumentException("Missing stage"));
            return false;
        }
        if (patchBody.taskInfo.stage == TaskStage.STARTED && patchBody.subStage == null) {
            patch.fail(new IllegalArgumentException("Missing substage"));
            return false;
        }
        if (patchBody.taskInfo.stage == TaskStage.CREATED) {
            patch.fail(new IllegalArgumentException("Did not expect to receive CREATED stage"));
            return false;
        }
        if (currentTask.taskInfo != null && currentTask.taskInfo.stage != null) {
            if (currentTask.taskInfo.stage.ordinal() > patchBody.taskInfo.stage.ordinal()) {
                patch.fail(new IllegalArgumentException("Task stage cannot move backwards"));
                return false;
            }
            if (currentTask.taskInfo.stage == TaskStage.STARTED
                    && patchBody.taskInfo.stage == TaskStage.STARTED) {
                if (currentTask.subStage.ordinal() > patchBody.subStage.ordinal()) {
                    patch.fail(new IllegalArgumentException("Task substage cannot move backwards"));
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * This updates the state of the task. Note that we are merging information from the
     * PATCH into the current task. Because we are merging into the current task (it's the
     * same object), we do not need to explicitly save the state: that will happen when
     * we call patch.complete()
     */
    private void updateState(Operation patch,
            ExampleTaskServiceState currentTask,
            ExampleTaskServiceState patchBody) {
        Utils.mergeWithState(getDocumentTemplate().documentDescription, currentTask, patchBody);

        // Take the new document expiration time
        if (currentTask.documentExpirationTimeMicros == 0) {
            currentTask.documentExpirationTimeMicros = patchBody.documentExpirationTimeMicros;
        }
    }

    /**
     * Handle SubStage QUERY_EXAMPLES.
     *
     * Query the query task service for all example services that we are authorized to access.
     * Authorization is implicit: we do not need to indicate it here.
     */
    private void handleQueryExamples(ExampleTaskServiceState task) {
        // Create a query for "all documents with kind ==
        // com:vmware:xenon:services:common:ExampleService:ExampleServiceState"
        Query exampleDocumentQuery = Query.Builder.create()
                .setTerm(ServiceDocument.FIELD_NAME_KIND,
                        Utils.buildKind(ExampleServiceState.class))
                .build();
        task.exampleQueryTask = QueryTask.Builder.createDirectTask()
                .setQuery(exampleDocumentQuery)
                .build();

        // Send the query to the query task service.
        // When we get a response, advance to the next substage, deleting examples
        // Note that we inherited the authorization context of the incoming patch, so
        // we will only see documents that can be seen by the requesting user.
        // The same is true of our completion: we'll continue to use the same authorization
        // context
        URI queryTaskUri = UriUtils.buildUri(this.getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
        Operation queryRequest = Operation.createPost(queryTaskUri)
                .setBody(task.exampleQueryTask)
                .setCompletion(
                        (op, ex) -> {
                            if (ex != null) {
                                logWarning("Query failed, task will not finish: %s",
                                        ex.getMessage());
                                return;
                            }
                            // We extract the result of the task because DELETE_EXAMPLES will use
                            // the list of documents found
                            task.exampleQueryTask = op.getBody(QueryTask.class);
                            sendSelfPatch(task, TaskStage.STARTED, SubStage.DELETE_EXAMPLES);
                        });
        sendRequest(queryRequest);
    }

    /**
     * Handle SubStage DELETE_EXAMPLES
     *
     * Delete all of the example service documents that we found in the QUERY_EXAMPLES substage.
     * We do all the DELETEs in parallel by using OperationJoin.
     */
    private void handleDeleteExamples(ExampleTaskServiceState task) {
        if (task.exampleQueryTask.results == null) {
            sendSelfFailurePatch(task, "Query task service returned null results");
            return;
        }

        if (task.exampleQueryTask.results.documentLinks == null) {
            sendSelfFailurePatch(task, "Query task service returned null documentLinks");
            return;
        }
        if (task.exampleQueryTask.results.documentLinks.size() == 0) {
            logInfo("No example service documents found, nothing to do");
            sendSelfPatch(task, TaskStage.FINISHED, null);
        }

        List<Operation> deleteOperations = new ArrayList<>();
        for (String exampleService : task.exampleQueryTask.results.documentLinks) {
            URI exampleServiceUri = UriUtils.buildUri(this.getHost(), exampleService);
            Operation deleteOp = Operation.createDelete(exampleServiceUri);
            deleteOperations.add(deleteOp);
        }

        // OperationJoin lets us do a set of operations in parallel. If we wanted to,
        // we could specify a batch size to limit the parallelism. We'll receive one
        // completion when all the operations complete.
        OperationJoin operationJoin = OperationJoin.create();
        operationJoin
                .setOperations(deleteOperations)
                .setCompletion((ops, exs) -> {
                    if (exs != null && !exs.isEmpty()) {
                        sendSelfFailurePatch(task, String.format("%d deletes failed", exs.size()));
                        return;
                    } else {
                        sendSelfPatch(task, TaskStage.FINISHED, null);
                    }
                }).sendWith(this);
    }


    /**
     * Send ourselves a PATCH that will indicate failure
     */
    private void sendSelfFailurePatch(ExampleTaskServiceState task, String failureMessage) {
        task.failureMessage = failureMessage;
        sendSelfPatch(task, TaskStage.FAILED, null);
    }

    /**
     * Send ourselves a PATCH that will advance to another step in the task workflow to the
     * specified stage and substage.
     */
    private void sendSelfPatch(ExampleTaskServiceState task, TaskStage stage, SubStage subStage) {
        if (task.taskInfo == null) {
            task.taskInfo = new TaskState();
        }
        task.taskInfo.stage = stage;
        task.subStage = subStage;
        sendSelfPatch(task);
    }

    /**
     * Send ourselves a PATCH. The caller is responsible for creating the PATCH body
     */
    private void sendSelfPatch(ExampleTaskServiceState task) {
        Operation patch = Operation.createPatch(getUri())
                .setBody(task)
                .setCompletion(
                        (op, ex) -> {
                            if (ex != null) {
                                logWarning("Failed to send patch, task has failed: %s",
                                        ex.getMessage());
                            }
                        });
        sendRequest(patch);
    }
}
