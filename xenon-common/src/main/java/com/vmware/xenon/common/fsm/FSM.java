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
import java.util.Set;

/**
 * A Finite State Machine with states of Type T and transitions (labels) of type R.
 * The FSM should be initialized by invoking configure() before other usage.
 */
public interface FSM<T, R> {

    void configure(Map<T, Map<R, T>> fsmConfig, T initialState);

    boolean isTransitionValid(R transition);

    boolean isNextState(T target);

    T getNextState(R transition);

    R getTransition(T target);

    Set<R> getTransitions();

    Collection<T> getNextStates();

    T getCurrentState();

    void adjustState(R transition);

}
