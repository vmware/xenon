/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.test;

import com.vmware.xenon.common.Operation;

/**
 * Represent result of {@link Operation}.
 *
 * This holds the {@link Operation} and {@link Throwable failure}.
 *
 * Performing verification(assertions) in callback({@link com.vmware.xenon.common.Operation.CompletionHandler})
 * is NOT recommended because thrown test failure is swallowed by the
 * "operation.[complete|fail]" methods.
 * Instead, it should take operation and failure in callback to outside, then
 * perform verification there.
 * This class serves the purpose of bringing operation and failure to outside.
 */
public class OperationResponse {

    public Operation operation;
    public Throwable failure;

    /**
     * Shortcut to retrieve body from {@link Operation} response.
     */
    public <T> T getBody(Class<T> type) {
        return this.operation.getBody(type);
    }

    public boolean hasOperation() {
        return this.operation != null;
    }

    public boolean hasFailure() {
        return this.failure != null;
    }

    public boolean isSuccess() {
        return this.failure == null;
    }

    public boolean isFailure() {
        return this.failure != null;
    }
}
