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
import java.util.function.Consumer;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.Utils;

/**
 * Contains boilerplate logic and state that most {@code TaskService} implementations might need.
 * This allows subclasses to focus more on their task-specific logic. While helpful, this base class
 * is certainly not required for task service implementations.
 */
public abstract class TaskService<T extends TaskService.TaskServiceState>
        extends StatefulService {

    /**
     * Defaults to expire a task instance if not completed in 4 hours. Subclasses can override
     */
    public static final long DEFAULT_EXPIRATION_MINUTES = TimeUnit.HOURS.toMinutes(4);

    /**
     * Extending {@code TaskService} implementations likely want to also provide their own {@code
     * State} PODO object that extends from this one. Minimally, they'll likely need to provide
     * their own {@code SubStage}.
     *
     * @see com.vmware.xenon.services.common.ExampleTaskService.ExampleTaskServiceState
     */
    public abstract static class TaskServiceState extends ServiceDocument {

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
     * Simple passthrough to our parent's constructor.
     */
    protected TaskService(Class<? extends ServiceDocument> stateType) {
        super(stateType);
    }

    /**
     * Helper method so a task can easily set its expiration time,
     * {@link ServiceDocument#documentExpirationTimeMicros}
     * <br/>
     * For example, to set the task's {@code state} to expire in 30 seconds, you would use:
     * <pre>
     *     setExpiration(state, 30, TimeUnit.SECONDS);
     * </pre>
     *
     * @param state         the state representation of the task. The {@code
     *                      state.documentExpirationTimeMicros} will be updated accordingly.
     * @param timeUnit      the unit of measure to set the expiration (ie: SECONDS, MINUTES, etc)
     * @param timeUnitValue the value
     */
    protected void setExpiration(T state, long timeUnitValue, TimeUnit timeUnit) {
        if (state == null) {
            throw new IllegalArgumentException("state cannot be null");
        }
        state.documentExpirationTimeMicros = Utils.getNowMicrosUtc() + timeUnit.toMicros(timeUnitValue);
    }

    /**
     * This handles the initial {@code POST} that creates the task service. Most subclasses won't
     * need to override this method, although they likely want to override the {@link
     * #validateStartPost(Operation)} and {@link #initializeState(TaskServiceState, Operation)}
     * methods.
     */
    @Override
    public void handleStart(Operation taskOperation) {
        T task = validateStartPost(taskOperation);
        if (task == null) {
            return;
        }
        taskOperation.complete();

        if (!ServiceHost.isServiceCreate(taskOperation)
                || (task.taskInfo != null && !TaskState.isCreated(task.taskInfo))) {
            // Skip self patch to STARTED if this is a restart operation, or, task stage is
            // other than CREATED.
            // Tasks that handle restart should override handleStart and decide if they should
            // continue processing on restart, or fail
            return;
        }

        initializeState(task, taskOperation);

        sendSelfPatch(task);
    }

    /**
     * Ensure that the initial input task is valid. Subclasses might want to override this
     * implementation to also validate their {@code SubStage}.
     */
    protected T validateStartPost(Operation taskOperation) {
        T task = getBody(taskOperation);

        if (!taskOperation.hasBody()) {
            taskOperation.fail(new IllegalArgumentException("POST body is required"));
            return null;
        }

        if (!ServiceHost.isServiceCreate(taskOperation)) {
            // we apply validation only on the original, client issued POST, not operations
            // caused by host restart
            return task;
        }

        if (task.taskInfo != null) {
            taskOperation.fail(new IllegalArgumentException(
                    "Do not specify taskInfo: internal use only"));
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

        // Put in some default expiration time if it hasn't been provided yet.
        if (task.documentExpirationTimeMicros == 0) {
            setExpiration(task, DEFAULT_EXPIRATION_MINUTES, TimeUnit.MINUTES);
        }

        // Subclasses should initialize their "SubStage"...
        taskOperation.setBody(task);
    }

    /**
     * Send ourselves a PATCH. The caller is responsible for creating the PATCH body
     */
    protected void sendSelfPatch(T task) {
        Operation patch = Operation.createPatch(getUri())
                .setBody(task);
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
        Utils.mergeWithState(getStateDescription(), currentTask, patchBody);
        // NOTE: If patchBody provides a new expiration, Utils.mergeWithState will take care of it
    }

    /**
     * Send ourselves a PATCH that will indicate failure
     */
    protected void sendSelfFailurePatch(T task, String failureMessage) {
        task.failureMessage = failureMessage;

        if (task.taskInfo == null) {
            task.taskInfo = new TaskState();
        }
        task.taskInfo.stage = TaskState.TaskStage.FAILED;
        sendSelfPatch(task);
    }

    /**
     * Send ourselves a PATCH that will advance to another step in the task workflow to the
     * specified stage and substage.
     *
     * @param taskState the task's state to use for the PATCH
     * @param stage the next stage to advance to
     * @param updateTaskState lambda helper for setting any custom field in the task's state (such
     *                        as SubStage). If null, it will be ignored.
     */
    protected void sendSelfPatch(T taskState, TaskState.TaskStage stage, Consumer<T> updateTaskState) {
        if (taskState.taskInfo == null) {
            taskState.taskInfo = new TaskState();
        }
        taskState.taskInfo.stage = stage;

        if (updateTaskState != null) {
            updateTaskState.accept(taskState);
        }
        sendSelfPatch(taskState);
    }
}
