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

package com.vmware.xenon.services.common;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.ServiceHost.ServiceNotFoundException;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.services.common.AuthorizationContextServiceHelper.AuthServiceContext;

/**
 * The authorization context service takes an operation's authorization context and
 * populates it with the user's roles and the associated resource queries.
 * As these involve relatively costly operations, extensive caching is applied.
 * If an inbound authorization context is keyed in this class's LRU then it
 * can be applied immediately.
 *
 * Entries in the LRU can be selectively cleared if any of their components
 * is updated. The responsibility for this lies with those services.
 * They will notify the authorization context service whenever they are updated.
 *
 * Entries in the LRU are invalidated whenever relevant user group, resource group,
 * or role, is updated, or removed. As new roles might apply to any of the users,
 * the entire cache is cleared whenever a new role is added.
 */
public class AuthorizationContextService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_AUTHZ_VERIFICATION;

    public AuthorizationContextService() {
        this.context = new AuthServiceContext(this, null, (requestBody) -> {
            synchronized (this.pendingOperationsBySubject) {
                if (this.pendingOperationsBySubject.containsKey(requestBody.subjectLink)) {
                    this.cacheClearRequests.add(requestBody.subjectLink);
                }
            }
        });
    }

    // auth service context used to pass the context of this service to AuthorizationContextServiceHelper
    private final AuthServiceContext context;
    // Collection of requests to clear the cache for a subject
    private final Set<String> cacheClearRequests = Collections.synchronizedSet(new HashSet<>());
    // Collection of pending operations as we already have a request
    private final Map<String, Collection<Operation>> pendingOperationsBySubject = new HashMap<>();

    /**
     * The service host will invoke this method to allow a service to handle
     * the request in-line or indicate it should be scheduled by service host.
     *
     * @return True if the request has been completed in-line.
     *         False if the request should be scheduled for execution by the service host.
     */
    @Override
    public  boolean queueRequest(Operation op) {
        return AuthorizationContextServiceHelper.queueRequest(op, this.context);
    }

    @Override
    public void handleRequest(Operation op) {
        if (op.getAction() == Action.DELETE && op.getUri().getPath().equals(getSelfLink())) {
            super.handleRequest(op);
            return;
        }
        AuthorizationContext ctx = op.getAuthorizationContext();
        if (ctx == null) {
            op.fail(new IllegalArgumentException("no authorization context"));
            return;
        }

        Claims claims = ctx.getClaims();
        if (claims == null) {
            op.fail(new IllegalArgumentException("no claims"));
            return;
        }
        String subject = claims.getSubject();
        // Add operation to collection of operations for this user.
        // Only if there was no collection for this subject will the routine
        // to gather state and roles for the subject be kicked off.
        synchronized (this.pendingOperationsBySubject) {
            Collection<Operation> pendingOperations = this.pendingOperationsBySubject.get(subject);
            if (pendingOperations != null) {
                pendingOperations.add(op);
                return;
            }

            // Nothing in flight for this subject yet, add new collection
            pendingOperations = new LinkedList<>();
            pendingOperations.add(op);
            this.pendingOperationsBySubject.put(subject, pendingOperations);
        }
        handlePopulateAuthContextCompletion(op);
        AuthorizationContextServiceHelper.populateAuthContext(op, this.context);
    }

    // this method nests a completion that will be invoked once the call to populateAuthContext
    // marks the input Operation complete. If the populated auth context can be used all
    // pending operations are notified; else we retry to populate the auth context again
    private void handlePopulateAuthContextCompletion(Operation op) {
        AuthorizationContext ctx = op.getAuthorizationContext();
        Claims claims = ctx.getClaims();
        String subject = claims.getSubject();
        op.nestCompletion((nestOp, nestEx) -> {
            if (nestEx != null) {
                failThrowable(subject, nestEx, this.context);
                return;
            }
            if (this.cacheClearRequests.remove(claims.getSubject())) {
                handlePopulateAuthContextCompletion(op);
                AuthorizationContextServiceHelper.populateAuthContext(op, this.context);
                return;
            }
            AuthorizationContext populatedAuthContext = nestOp.getAuthorizationContext();
            getHost().cacheAuthorizationContext(this, populatedAuthContext);
            completePendingOperations(subject, populatedAuthContext, this.context);
        });
    }

    private Collection<Operation> getPendingOperations(String subject, AuthServiceContext context) {
        Collection<Operation> operations;
        synchronized (this.pendingOperationsBySubject) {
            operations = this.pendingOperationsBySubject.remove(subject);
        }
        if (operations == null) {
            return Collections.emptyList();
        }

        return operations;
    }

    private void completePendingOperations(String subject, AuthorizationContext ctx,
            AuthServiceContext context) {
        for (Operation op : getPendingOperations(subject, context)) {
            this.setAuthorizationContext(op, ctx);
            op.complete();
        }
    }

    private void failThrowable(String subject, Throwable e, AuthServiceContext context) {
        if (e instanceof ServiceNotFoundException) {
            failNotFound(subject, context);
            return;
        }
        for (Operation op : getPendingOperations(subject, context)) {
            op.fail(e);
        }
    }

    private void failNotFound(String subject, AuthServiceContext context) {
        for (Operation op : getPendingOperations(subject, context)) {
            op.fail(Operation.STATUS_CODE_NOT_FOUND);
        }
    }
}
