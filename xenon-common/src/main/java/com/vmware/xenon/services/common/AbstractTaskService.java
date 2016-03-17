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

import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.Utils;

/**
 * Contains boilerplate logic and state that most {@code TaskService} implementations might need.
 * This allows subclasses to focus more on their task-specific logic. While helpful, this base class
 * is certainly not required for task service implementations.
 */
public abstract class AbstractTaskService<T extends AbstractTaskService.BaseTaskServiceState>
        extends StatefulService {

    /**
     * Extending {@code TaskService} implementations likely want to also provide their own {@code
     * State} PODO object that extends from this one. Minimally, they'll likely need to provide
     * their own {@code SubStage}.
     *
     * @see com.vmware.xenon.services.common.ExampleTaskService.ExampleTaskServiceState
     */
    public abstract static class BaseTaskServiceState extends ServiceDocument {

        /**
         * Tracks progress of the task. Should not be manipulated by clients.
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public TaskState taskInfo;

        /**
         * If taskInfo.stage == FAILED, this message will say why
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String failureMessage;
    }

    /**
     * Defaults to expire a task instance if not completed in 4 hours. Subclasses can override
     * this.
     */
    protected int minutesBeforeExpire = 60 * 4;

    /**
     * Simple passthrough to our parent's constructor.
     */
    protected AbstractTaskService(Class<? extends ServiceDocument> stateType) {
        super(stateType);
    }

    /**
     * This handles the initial {@code POST} that creates the task service. Most subclasses won't
     * need to override this method, although they likely want to override the {@link
     * #validateStartPost(Operation)} and {@link #initializeState(BaseTaskServiceState, Operation)}
     * methods.
     */
    @Override
    public void handleStart(Operation taskOperation) {
        T task = validateStartPost(taskOperation);
        if (task == null) {
            return;
        }
        taskOperation.complete();

        initializeState(task, taskOperation);
        sendSelfPatch(task);
    }

    /**
     * Ensure that the initial input task is valid. Subclasses might want to override this
     * implementation to also validate their {@code SubStage}.
     */
    protected T validateStartPost(Operation taskOperation) {
        if (!taskOperation.hasBody()) {
            taskOperation.fail(new IllegalArgumentException("POST body is required"));
            return null;
        }

        T task = getBody(taskOperation);
        if (task.taskInfo != null) {
            taskOperation.fail(new IllegalArgumentException(
                    "Do not specify taskBody: internal use only"));
            return null;
        }

        // Subclasses might also want to ensure that their "SubStage" is not specified also
        return task;
    }

    /**
     * Initialize the task with default values. Subclasses might want to override this
     * implementation to initialize their {@code SubStage}
     */
    protected void initializeState(T task, Operation taskOperation) {
        task.taskInfo = new TaskState();
        task.taskInfo.stage = TaskState.TaskStage.STARTED;

        // Put in some default expiration time.
        task.documentExpirationTimeMicros =
                Utils.getNowMicrosUtc() + TimeUnit.MINUTES.toMicros(this.minutesBeforeExpire);

        // Subclasses should initialize their "SubStage"...
        taskOperation.setBody(task);
    }

    /**
     * All task-specific logic is handled via {@code PATCH}es. Subclasses need to provide their
     * task-specific logic here.
     */
    @Override
    public abstract void handlePatch(Operation patch);

    /**
     * Send ourselves a PATCH. The caller is responsible for creating the PATCH body
     */
    protected void sendSelfPatch(T task) {
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

    /**
     * Validate that the PATCH we got requests reasonable changes to our state. Subclasses might
     * want to override this implementation to validate their custom state, such as {@code
     * SubStage}.
     */
    protected boolean validateTransition(Operation patch, T currentTask, T patchBody) {
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
        if (currentTask.taskInfo != null && currentTask.taskInfo.stage != null) {
            if (currentTask.taskInfo.stage.ordinal() > patchBody.taskInfo.stage.ordinal()) {
                patch.fail(new IllegalArgumentException("Task stage cannot move backwards"));
                return false;
            }
        }

        // Subclasses should validate transitions to their "SubStage" as well...
        return true;
    }

    /**
     * This updates the state of the task. Note that we are merging information from the PATCH into
     * the current task. Because we are merging into the current task (it's the same object), we do
     * not need to explicitly save the state: that will happen when we call patch.complete()
     */
    protected void updateState(T currentTask, T patchBody) {
        Utils.mergeWithState(getDocumentTemplate().documentDescription, currentTask, patchBody);

        // Take the new document expiration time
        if (currentTask.documentExpirationTimeMicros == 0) {
            currentTask.documentExpirationTimeMicros = patchBody.documentExpirationTimeMicros;
        }
    }
}
