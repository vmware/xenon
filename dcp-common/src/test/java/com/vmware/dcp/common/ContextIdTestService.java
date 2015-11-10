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

package com.vmware.dcp.common;

import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.Override;
import java.lang.Throwable;
import java.util.Objects;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.TaskState;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.common.test.TestProperty;

public class ContextIdTestService extends StatefulService {

    public static class State extends ServiceDocument {
        public String startContextId;
        public String getContextId;
        public String patchContextId;
        public String putContextId;

        public TaskState taskInfo;
    }

    public ContextIdTestService() {
        super(State.class);
        super.toggleOption(ServiceOption.CONCURRENT_UPDATE_HANDLING, true);
    }

    @Override
    public void handleStart(Operation op) {
        try {
            State state = op.getBody(State.class);
            validate(op, state.startContextId, "handleStart");
            op.complete();
        } catch (Throwable e) {
            logSevere(e);
            op.fail(e);
        }
    }

    @Override
    public void handleGet(Operation op) {
        try {
            State state = getState(op);

            validate(op, state.getContextId, "handleGet");
            super.handleGet(op);
        } catch (Throwable e) {
            logSevere(e);
            op.fail(e);
        }
    }

    @Override
    public void handlePatch(Operation op) {
        try {
            State state = getState(op);
            State patch = op.getBody(State.class);

            applyPatch(state, patch);
            validate(op, state.patchContextId, "handlePatch");
            op.complete();

            if (Objects.equals(op.getReferer().getPath(), getUri().getPath())) {
                // if this was a termination patch just exit
                return;
            }

            // send a get request to churn the context id
            this.sendRequest(Operation.createGet(getUri()).setContextId(state.getContextId));

            // send self patch
            this.sendSelfPatch(new State(), null, (o, e) -> {
                if (e != null) {
                    fail(e);
                    return;
                }

                try {
                    logFine("handleCompletion(%s) op cid[%s]", o.getId(), o.getContextId());
                    validate(o, state.patchContextId, "patchCompletion");

                    this.sendStageUpdate(TaskState.TaskStage.FINISHED, null);
                } catch (Throwable ex) {
                    fail(ex);
                }
            });
        } catch (Throwable e) {
            logSevere(e);
            op.fail(e);
        }
    }

    @Override
    public void handlePut(Operation op) {
        try {
            State state = getState(op);
            validate(op, state.putContextId, "handlePut");
            op.complete();
        } catch (Throwable e) {
            logSevere(e);
            op.fail(e);
        }
    }

    private void applyPatch(State current, State patch) {
        if (patch.taskInfo != null) {
            current.taskInfo = patch.taskInfo;
        }
    }

    private void validate(Operation op, String expectedContextId, String caller) {
        logFine("%s(%s): expected cid[%s] op cid[%s] thread cid[%s]",
                caller, op.getId(), expectedContextId, op.getContextId(),
                OperationContext.getContextId());

        if (Objects.equals(
                op.getContextId(),
                TestProperty.DISABLE_CONTEXT_ID_VALIDATION.toString())) {
            return;
        }

        if (!Objects.equals(op.getContextId(), expectedContextId)) {
            throw new IllegalArgumentException(
                    String.format("%s(%s): operation contextId invalid: expected[%s] got[%s]",
                            caller,
                            op.getId(),
                            expectedContextId,
                            op.getContextId()));
        }

        if (!Objects.equals(OperationContext.getContextId(), expectedContextId)) {
            throw new IllegalStateException(
                    String.format("%s(%s): thread contextId invalid: expected[%s] got[%s]",
                            caller,
                            op.getId(),
                            expectedContextId,
                            OperationContext.getContextId()));
        }
    }

    private void fail(Throwable e) {
        logSevere(e);
        this.sendStageUpdate(TaskState.TaskStage.FAILED, e);
    }

    private void sendStageUpdate(TaskState.TaskStage stage, Throwable e) {
        State fail = new State();
        fail.taskInfo = new TaskState();
        fail.taskInfo.stage = stage;
        if (e != null) {
            fail.taskInfo.failure = Utils.toServiceErrorResponse(e);
        }

        this.sendSelfPatch(fail, TestProperty.DISABLE_CONTEXT_ID_VALIDATION.toString(), null);
    }

    private void sendSelfPatch(State state, String contextId, CompletionHandler handler) {
        this.sendRequest(Operation
                .createPatch(getUri())
                .forceRemote()
                .setBody(state)
                .setReferer(getUri())
                .setContextId(contextId)
                .setCompletion(handler));
    }
}
