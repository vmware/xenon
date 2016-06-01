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
public class OperationContext {

    private AuthorizationContext authContext;
    private String contextId;
    private String transactionId;

    private OperationContext(AuthorizationContext authContext, String contextId, String transactionId) {
        this.authContext = authContext;
        this.contextId = contextId;
        this.transactionId = transactionId;
    }

    /**
     * Variable to store the contextId in thread-local
     */
    private static final ThreadLocal<String> threadContextId = new ThreadLocal<>();

    /**
     * Variable to store the transactionId in thread-local
     */
    private static final ThreadLocal<String> threadTransactionId = new ThreadLocal<>();

    private static final ThreadLocal<AuthorizationContext> threadAuthContext = new ThreadLocal<>();

    public static void setContextId(String contextId) {
        threadContextId.set(contextId);
    }

    public static String getContextId() {
        return threadContextId.get();
    }

    static void setAuthorizationContext(AuthorizationContext ctx) {
        threadAuthContext.set(ctx);
    }

    public static void setTransactionId(String transactionId) {
        threadTransactionId.set(transactionId);
    }

    public static String getTransactionId() {
        return threadTransactionId.get();
    }

    /**
     * Sets current thread's authorization context based on {@code op} headers and/or cookies.
     *
     * @param host Service host.
     * @param op   Operation containing authorization headers / cookies.
     */
    public static void setAuthorizationContext(ServiceHost host, Operation op) {
        setAuthorizationContext(host.getAuthorizationContext(op));
    }

    public static AuthorizationContext getAuthorizationContext() {
        return threadAuthContext.get();
    }

    /**
     * Get the OperationContext associated with the thread
     * @return OperationContext instance
     */
    public static OperationContext getOperationContext() {
        return new OperationContext(threadAuthContext.get(), threadContextId.get(), threadTransactionId.get());
    }

    /**
     * Set the OperationContext associated with the thread based on the specified OperationContext
     * @param opContext Input OperationContext
     */
    public static void setFrom(OperationContext opContext) {
        threadAuthContext.set(opContext.authContext);
        threadContextId.set(opContext.contextId);
        threadTransactionId.set(opContext.transactionId);
    }

    /**
     * Set the OperationContext associated with the thread based on the specified Operation
     * @param op Operation to build the OperationContext
     */
    public static void setFrom(Operation op) {
        threadAuthContext.set(op.getAuthorizationContext());
        threadContextId.set(op.getContextId());
        threadTransactionId.set(op.getTransactionId());
    }

    /**
     * reset the OperationContext associated with the thread
     */
    public static void reset() {
        threadAuthContext.set(null);
        threadContextId.set(null);
        threadTransactionId.set(null);
    }

    /**
     * Restore the OperationContext associated with this thread to the value passed in
     * @param ctx OperationContext instance to restore to
     */
    public static void restoreOperationContext(OperationContext ctx) {
        setAuthorizationContext(ctx.authContext);
        setContextId(ctx.contextId);
        setTransactionId(ctx.transactionId);
    }
}
