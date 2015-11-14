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

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A basic implementation of a Finite State Machine. The FSM is initialized by calling its configure() method, providing a map of states
 * and transitions to other states; and the initial state. Then consumer can check if a transition is valid, adjust the current state, etc.
 */
public class FSMTracker<T, R> implements FSM<T, R> {

    private Map<T, Map<R, T>> config;
    private T currentState;

    public FSMTracker() {
    }

    @Override
    public void configure(Map<T, Map<R, T>> fsmConfig, T initialState) {
        this.config = fsmConfig;
        this.currentState = initialState;

        validate();
    }

    @Override
    public R getTransition(T target) {
        validate();

        Map<R, T> transitions = this.config.get(this.currentState);
        for (Entry<R, T> e : transitions.entrySet()) {
            if (e.getValue() == target) {
                return e.getKey();
            }
        }

        throw new IllegalArgumentException(String.format(
                "State %s cannot be reached from state %s", target, this.currentState));
    }

    @Override
    public boolean isNextState(T target) {
        validate();

        try {
            getTransition(target);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    @Override
    public boolean isTransitionValid(R transition) {
        validate();

        Map<R, T> transitions = this.config.get(this.currentState);
        return transitions.containsKey(transition);
    }

    @Override
    public T getNextState(R transition) {
        validate();

        Map<R, T> transitions = this.config.get(this.currentState);
        T nextState = transitions.get(transition);
        if (nextState == null) {
            throw new IllegalArgumentException(String.format("Transition %s is not found",
                    transition));
        }

        return nextState;
    }

    @Override
    public Set<R> getTransitions() {
        validate();

        Map<R, T> transitions = this.config.get(this.currentState);
        return transitions.keySet();
    }

    @Override
    public Collection<T> getNextStates() {
        validate();

        Map<R, T> transitions = this.config.get(this.currentState);
        return transitions.values();
    }

    @Override
    public T getCurrentState() {
        validate();

        return this.currentState;
    }

    @Override
    public void adjustState(R transition) {
        validate();

        this.currentState = getNextState(transition);
    }

    private void validate() {
        if (this.config == null) {
            throw new IllegalStateException("state machine config is null");
        }

        if (this.currentState == null) {
            throw new IllegalStateException("state machine current state is null");
        }

        if (!this.config.containsKey(this.currentState)) {
            throw new IllegalStateException(String.format(
                    "state machine config does not contain current state %s", this.currentState));
        }
    }

}
