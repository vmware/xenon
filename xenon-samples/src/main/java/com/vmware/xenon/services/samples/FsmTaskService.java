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

package com.vmware.xenon.services.samples;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.fsm.TaskFSMTracker;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * This service provides a simple demonstration for the usage of TaskFSM to validate input against current state and to adjust state.
 * For simplicity, the same type is used to both manage the internal state as well as the state transferred through the REST calls.
 * State is updated synchronously in this sample.
 */
public class FsmTaskService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.SAMPLES + "/fsmTask";

    public static Service createFactory() {
        return FactoryService.create(FsmTaskService.class);
    }

    public static class FsmTaskServiceState extends ServiceDocument {
        public TaskFSMTracker fsmInfo;
        public TaskState taskInfo;
    }

    public FsmTaskService() {
        super(FsmTaskServiceState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation start) {
        try {
            validateAndFixInitialState(start);
            start.complete();
        } catch (Exception e) {
            start.fail(e);
        }
    }

    @Override
    public void handlePatch(Operation patch) {
        FsmTaskServiceState currentState = getState(patch);
        FsmTaskServiceState newState = patch.getBody(FsmTaskServiceState.class);

        try {
            validateStateTransitionAndInput(currentState, newState);
        } catch (Exception e) {
            patch.fail(e);
            return;
        }

        adjustState(patch, currentState, newState.taskInfo.stage);
        patch.complete();

        if (TaskStage.STARTED.equals(newState.taskInfo.stage)) {
            // optionally perform long-running operations, as we've already completed the client operation
        }
    }

    private void validateAndFixInitialState(Operation start) {
        FsmTaskServiceState state = null;

        // client does not have to provide an initial state, but if it provides one it has to be valid
        if (start.hasBody()) {
            state = start.getBody(FsmTaskServiceState.class);
            if (state == null || state.taskInfo == null) {
                throw new IllegalArgumentException(
                        "attempt to initialize service with an empty state");
            }
            if (!TaskStage.CREATED.equals(state.taskInfo.stage)) {
                throw new IllegalArgumentException(String.format(
                        "attempt to initialize service with stage %s != CREATED",
                        state.taskInfo.stage));
            }
        } else {
            state = new FsmTaskServiceState();
            state.taskInfo = new TaskState();
            state.taskInfo.stage = TaskStage.CREATED;
        }

        state.fsmInfo = new TaskFSMTracker();

        start.setBody(state);
    }

    private void validateStateTransitionAndInput(FsmTaskServiceState currentState,
            FsmTaskServiceState newState) {
        if (newState == null || newState.taskInfo == null || newState.taskInfo.stage == null) {
            throw new IllegalArgumentException("new stage is null");
        }

        if (!currentState.fsmInfo.isTransitionValid(newState.taskInfo.stage)) {
            throw new IllegalArgumentException(String.format(
                    "Illegal state transition: current stage=%s, desired stage=%s",
                    currentState.fsmInfo.getCurrentState(),
                    newState.taskInfo.stage));
        }
    }

    private void adjustState(Operation o, FsmTaskServiceState currentState, TaskStage stage) {
        currentState.fsmInfo.adjustState(stage);
        currentState.taskInfo.stage = stage;
        super.setState(o, currentState);

    }

}
