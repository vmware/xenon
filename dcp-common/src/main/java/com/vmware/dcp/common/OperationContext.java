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

import com.vmware.dcp.common.Operation.AuthorizationContext;

/**
 * OperationContext encapsulates the runtime context of an Operation
 * The context is maintained as a thread local variable that is set
 * by the service host or the Operation object
 * OperationContext instances are immutable.
 */
public class OperationContext {

    private AuthorizationContext authContext;
    private String contextId;

    private OperationContext(AuthorizationContext authContext, String contextId) {
        this.authContext = authContext;
        this.contextId = contextId;
    }

    /**
     * Variable to store the contextId in thread-local
     */
    private static final ThreadLocal<String> threadContextId = new ThreadLocal<>();

    static void setContextId(String contextId) {
        threadContextId.set(contextId);
    }

    public static String getContextId() {
        return threadContextId.get();
    }

    private static final ThreadLocal<AuthorizationContext> threadAuthContext = new ThreadLocal<>();

    static void setAuthorizationContext(AuthorizationContext ctx) {
        threadAuthContext.set(ctx);
    }

    public static AuthorizationContext getAuthorizationContext() {
        return threadAuthContext.get();
    }

    /**
     * Get the OperationContext associated with the thread
     * @return OperationContext instance
     */
    public static OperationContext getOperationContext() {
        return new OperationContext(threadAuthContext.get(), threadContextId.get());
    }

    /**
     * Restore the OperationContext associated with this thread to the value passed in
     * @param ctx OperationContext instance to restore to
     */
    public static void restoreOperationContext(OperationContext ctx) {
        setAuthorizationContext(ctx.authContext);
        setContextId(ctx.contextId);
    }

}
