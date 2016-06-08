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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.TaskState.TaskStage;

public class TestTaskFSMTracker {

    private TaskFSMTracker fsm;

    @Before
    public void setUp() throws Exception {
        this.fsm = new TaskFSMTracker();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAll() {
        // initial stage is CREATED
        assertEquals(TaskStage.CREATED, this.fsm.getCurrentState());
        assertTrue(this.fsm.isTransitionValid(TaskStage.STARTED));
        assertTrue(this.fsm.isTransitionValid(TaskStage.FAILED));
        assertTrue(this.fsm.isTransitionValid(TaskStage.CANCELLED));
        assertFalse(this.fsm.isTransitionValid(TaskStage.FINISHED));

        // move to STARTED
        this.fsm.adjustState(TaskStage.STARTED);
        assertEquals(TaskStage.STARTED, this.fsm.getCurrentState());
        assertTrue(this.fsm.isTransitionValid(TaskStage.FAILED));
        assertTrue(this.fsm.isTransitionValid(TaskStage.CANCELLED));
        assertTrue(this.fsm.isTransitionValid(TaskStage.FINISHED));
        assertFalse(this.fsm.isTransitionValid(TaskStage.CREATED));

        // attempt to move back to CREATED
        boolean exceptionRaised = false;
        try {
            this.fsm.adjustState(TaskStage.CREATED);
        } catch (Exception ex) {
            exceptionRaised = true;
        }
        assertTrue(exceptionRaised);
        assertEquals(TaskStage.STARTED, this.fsm.getCurrentState());

        // move to FINISHED
        this.fsm.adjustState(TaskStage.FINISHED);
        assertEquals(TaskStage.FINISHED, this.fsm.getCurrentState());
    }

}
