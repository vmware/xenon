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
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFSMTracker {

    static final int NUM = 10;

    static class TestState {
        public int id;
    }

    private FSMTracker<TestState, String> fsm;

    @Before
    public void setUp() throws Exception {
        this.fsm = new FSMTracker<TestFSMTracker.TestState, String>();
        configure();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAll() {
        for (int i = 0; i < NUM; i++) {
            Set<String> transitions = this.fsm.getTransitions();
            assertEquals(1, transitions.size());

            String transition = transitions.toArray(new String[0])[0];
            assertTrue(this.fsm.isTransitionValid(transition));

            TestState nextState = this.fsm.getNextState(transition);
            assertEquals(transition, this.fsm.getTransition(nextState));

            int expectedId = i < (NUM - 1) ? this.fsm.getCurrentState().id + 1 : 0;
            assertEquals(expectedId, nextState.id);

            assertEquals(1, this.fsm.getNextStates().size());
            assertTrue(this.fsm.isNextState(nextState));

            this.fsm.adjustState(transition);
        }
    }

    private void configure() {
        // a simple loop
        Map<TestState, Map<String, TestState>> conf = new HashMap<>();
        TestState[] TestStates = new TestState[NUM];
        for (int i = 0; i < NUM; i++) {
            TestStates[i] = new TestState();
            TestStates[i].id = i;
        }

        for (int i = 0; i < NUM; i++) {
            Map<String, TestState> transitions = new HashMap<>();
            TestState target = i < (NUM - 1) ? TestStates[i + 1] : TestStates[0];
            transitions.put(String.format("move to %d", target.id), target);
            conf.put(TestStates[i], transitions);
        }

        this.fsm.configure(conf, TestStates[0]);
    }

}
