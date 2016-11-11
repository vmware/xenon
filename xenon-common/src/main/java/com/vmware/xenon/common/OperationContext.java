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

import com.vmware.xenon.common.Operation.AuthorizationContext;

/**
 * OperationContext encapsulates the runtime context of an Operation
 * The context is maintained as a thread local variable that is set
 * by the service host or the Operation object
 * OperationContext instances are immutable.
 */
public final class OperationContext implements Cloneable {

    /**
     * Variable to store the OperationContext in thread-local
     */
    private static final ThreadLocal<OperationContext> threadOperationContext = ThreadLocal.withInitial(
            OperationContext::new);

    AuthorizationContext authContext;
    String contextId;
    String transactionId;

    private OperationContext() {
    }

    public OperationContext clone() {
        try {
            return (OperationContext) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    static OperationContext getOperationContextNoCloning() {
        return threadOperationContext.get();
    }

    /**
     * Variable to store the transactionId in thread-local
     */
    public static void setContextId(String contextId) {
        threadOperationContext.get().contextId = contextId;
    }

    public static String getContextId() {
        return threadOperationContext.get().contextId;
    }

    static void setAuthorizationContext(AuthorizationContext ctx) {
        threadOperationContext.get().authContext = ctx;
    }

    public static void setTransactionId(String transactionId) {
        threadOperationContext.get().transactionId = transactionId;
    }

    public static String getTransactionId() {
        return threadOperationContext.get().transactionId;
    }

    public static AuthorizationContext getAuthorizationContext() {
        return threadOperationContext.get().authContext;
    }

    /**
     * Get the OperationContext associated with the thread
     * @return OperationContext instance
     */
    public static OperationContext getOperationContext() {
        return threadOperationContext.get().clone();
    }

    /**
     * Set the OperationContext associated with the thread based on the specified OperationContext
     * @param opCtx Input OperationContext
     */
    public static void setFrom(OperationContext opCtx) {
        OperationContext currentOpCtx = threadOperationContext.get();
        currentOpCtx.authContext = opCtx.authContext;
        currentOpCtx.transactionId = opCtx.transactionId;
        currentOpCtx.contextId = opCtx.contextId;
    }

    /**
     * Set the OperationContext associated with the thread based on the specified Operation
     * @param op Operation to build the OperationContext
     */
    public static void setFrom(Operation op) {
        OperationContext currentOpCtx = threadOperationContext.get();
        currentOpCtx.authContext = op.getAuthorizationContext();
        currentOpCtx.transactionId = op.getTransactionId();
        currentOpCtx.contextId = op.getContextId();
    }

    /**
     * reset the OperationContext associated with the thread
     */
    public static void reset() {
        OperationContext opCtx = threadOperationContext.get();
        opCtx.authContext = null;
        opCtx.transactionId = null;
        opCtx.contextId = null;
    }

    /**
     * Restore the OperationContext associated with this thread to the value passed in
     * @param opCtx OperationContext instance to restore to
     */
    public static void restoreOperationContext(OperationContext opCtx) {
        OperationContext currentOpCtx = threadOperationContext.get();
        currentOpCtx.authContext = opCtx.authContext;
        currentOpCtx.transactionId = opCtx.transactionId;
        currentOpCtx.contextId = opCtx.contextId;
    }
}
