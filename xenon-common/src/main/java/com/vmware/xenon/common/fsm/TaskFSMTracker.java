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

package com.vmware.xenon.common.fsm;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.vmware.xenon.common.TaskState.TaskStage;

/**
 * A Finite State Machine implementation for TaskStage.
 * A client does not need to explicitly invoke this class' configure(), only instantiate the class.
 */
public class TaskFSMTracker extends FSMTracker<TaskStage, TaskStage> implements TaskFSM {

    public TaskFSMTracker() {
        Map<TaskStage, Map<TaskStage, TaskStage>> config = new HashMap<>();

        // create states
        for (TaskStage s : EnumSet.allOf(TaskStage.class)) {
            config.put(s, new HashMap<>());
        }

        // create transitions
        for (Entry<TaskStage, Map<TaskStage, TaskStage>> e : config.entrySet()) {
            Map<TaskStage, TaskStage> transitions = e.getValue();

            switch (e.getKey()) {
            case CREATED:
                transitions.put(TaskStage.STARTED, TaskStage.STARTED);
                transitions.put(TaskStage.FAILED, TaskStage.FAILED);
                transitions.put(TaskStage.CANCELLED, TaskStage.CANCELLED);
                break;

            case STARTED:
                transitions.put(TaskStage.STARTED, TaskStage.STARTED);
                transitions.put(TaskStage.FINISHED, TaskStage.FINISHED);
                transitions.put(TaskStage.FAILED, TaskStage.FAILED);
                transitions.put(TaskStage.CANCELLED, TaskStage.CANCELLED);
                break;

            default:
                // note that FINISHED, FAILED and CANCELLED are terminal states
            }

            config.put(e.getKey(), transitions);
        }

        super.configure(config, TaskStage.CREATED);
    }

}
