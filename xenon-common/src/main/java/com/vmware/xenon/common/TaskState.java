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

package com.vmware.xenon.common;

/**
 * Describes a service task
 */
public class TaskState {

    public static enum TaskStage {
        /**
         * Task is created
         */
        CREATED,

        /**
         * Task has started processing
         */
        STARTED,

        /**
         * Task finished successfully
         */
        FINISHED,

        /**
         * Task failed, failure reason is in the failure property
         */
        FAILED,

        /**
         * Task was cancelled, cancellation reason is in the failure property
         */
        CANCELLED,
    }

    /**
     * Current stage of the query
     */
    public TaskStage stage;

    /**
     * Value indicating whether task should complete the creation POST only after its complete.
     * Client enables this at the risk of waiting for the POST and consuming a connection. It should
     * not be enabled for tasks that do long running I/O with other services
     */
    public boolean isDirect;

    /**
     * Failure description for tasks that terminate in FAILED stage
     */
    public ServiceErrorResponse failure;

    /**
     * Duration of the query execution.
     */
    public Long durationMicros;

    public static boolean isFailed(TaskState taskInfo) {
        return taskInfo.stage == TaskStage.FAILED;
    }

    public static boolean isFinished(TaskState taskInfo) {
        return taskInfo.stage == TaskStage.FINISHED;
    }

    public static boolean isCancelled(TaskState taskInfo) {
        return taskInfo.stage == TaskStage.CANCELLED;
    }
}
