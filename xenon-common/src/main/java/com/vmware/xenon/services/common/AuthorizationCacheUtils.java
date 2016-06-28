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

import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;

public class AuthorizationCacheUtils {

    /**
     * Helper method that nests a completion to clear the service host authz cache for the specified user service
     * The nested completion will run after the operation passed in has been marked complete outside of this
     * method
     * @param s service context to invoke the operation
     * @param op Operation to mark completion/failure
     * @param userLink UserService state
     */
    public static void clearAuthzCacheForUser(Service s, Operation op, String userLink) {
        op.nestCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            if (userLink != null) {
                s.getHost().clearAuthorizationContext(s, userLink);
            }
            op.complete();
        });
    }

    /**
     * Helper method that nests a completion to clear the service host authz cache for all
     * services that a UserGroup service query resolves to
     * The nested completion will run after the operation passed in has been marked complete outside of this
     * method
     * @param s service context to invoke the operation
     * @param op Operation to mark completion/failure
     * @param userGroupState UserGroup service state
     */
    public static void clearAuthzCacheForUserGroup(Service s, Operation op, UserGroupState userGroupState) {
        op.nestCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            if (userGroupState.query == null) {
                op.complete();
                return;
            }
            QueryTask queryTask = new QueryTask();
            queryTask.querySpec = new QuerySpecification();
            queryTask.querySpec.query = userGroupState.query;
            queryTask.setDirect(true);
            Operation postOp = Operation.createPost(s, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                    .setBody(queryTask)
                    .setCompletion((queryOp, queryEx) -> {
                        if (queryEx != null) {
                            op.fail(queryEx);
                            return;
                        }
                        QueryTask queryTaskResult = queryOp.getBody(QueryTask.class);
                        ServiceDocumentQueryResult result = queryTaskResult.results;
                        if (result.documentLinks == null || result.documentLinks.isEmpty()) {
                            op.complete();
                            return;
                        }
                        for (String userLink : result.documentLinks) {
                            s.getHost().clearAuthorizationContext(s, userLink);
                        }
                        op.complete();
                    }
                );
            s.setAuthorizationContext(postOp, s.getSystemAuthorizationContext());
            s.sendRequest(postOp);
        });
    }

    /**
     * Helper method that nests a completion to clear the service host authz cache for all
     * services that a Role service resolves to. A Role has a reference
     * to a UserGroup instance which instance points to users
     * The nested completion will run after the operation passed in has been marked complete outside of this
     * method
     * @param s service context to invoke the operation
     * @param op Operation to mark completion/failure
     * @param roleState Role service state
     */
    public static void clearAuthzCacheForRole(Service s, Operation op, RoleState roleState) {
        op.nestCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            if (roleState.userGroupLink == null) {
                op.complete();
                return;
            }
            Operation parentOp = Operation.createGet(s.getHost(), roleState.userGroupLink)
                    .setCompletion((getOp, getEx) -> {
                        // the userGroup link might not be valid; just mark the operation complete
                        if (getOp.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                            op.complete();
                            return;
                        }
                        if (getEx != null) {
                            op.setBodyNoCloning(getOp.getBodyRaw()).fail(getOp.getStatusCode());
                            return;
                        }
                        UserGroupState userGroupState = getOp.getBody(UserGroupState.class);
                        clearAuthzCacheForUserGroup(s, op, userGroupState);
                        op.complete();
                    });
            s.setAuthorizationContext(parentOp, s.getSystemAuthorizationContext());
            s.sendRequest(parentOp);
        });
    }

    /**
     * Helper method that nests a completion to clear the service host authz cache for all
     * services that a ResourceGroup service resolves to. A Role has a reference
     * to a ResourceGroup instance and a UserGroup instance. A single ResourceGroup
     * can be referenced by multiple Roles (and hence UserGroup instances)
     * The nested completion will run after the operation passed in has been marked complete outside of this
     * method
     * @param s service context to invoke the operation
     * @param op Operation to mark completion/failure
     * @param resourceGroupState ResourceGroup service state
     */
    public static void clearAuthzCacheForResourceGroup(Service s, Operation op, ResourceGroupState resourceGroupState) {
        op.nestCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            QueryTask queryTask = new QueryTask();
            queryTask.querySpec = new QuerySpecification();
            Query resourceGroupQuery = Builder.create()
                    .addFieldClause(
                            RoleState.FIELD_NAME_RESOURCE_GROUP_LINK,
                            resourceGroupState.documentSelfLink)
                    .addKindFieldClause(RoleState.class)
                    .build();
            queryTask.querySpec.options =
                    EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
            queryTask.setDirect(true);
            queryTask.querySpec.query = resourceGroupQuery;
            queryTask.setDirect(true);
            Operation postOp = Operation.createPost(s, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                    .setBody(queryTask)
                    .setCompletion((queryOp, queryEx) -> {
                        if (queryEx != null) {
                            op.fail(queryEx);
                            return;
                        }

                        QueryTask queryTaskResult = queryOp.getBody(QueryTask.class);
                        ServiceDocumentQueryResult result = queryTaskResult.results;
                        if (result.documents == null || result.documents.isEmpty()) {
                            op.complete();
                            return;
                        }
                        AtomicInteger completionCount = new AtomicInteger(0);
                        CompletionHandler handler = (subOp, subEx) -> {
                            if (subEx != null) {
                                op.fail(subEx);
                                return;
                            }
                            if (completionCount.incrementAndGet() == result.documents.size()) {
                                op.complete();
                            }
                        };
                        for (Object doc : result.documents.values()) {
                            RoleState roleState = Utils.fromJson(doc, RoleState.class);
                            Operation roleOp = new Operation();
                            roleOp.setCompletion(handler);
                            clearAuthzCacheForRole(s, roleOp, roleState);
                            roleOp.complete();
                        }
                    }
                );
            s.setAuthorizationContext(postOp, s.getSystemAuthorizationContext());
            s.sendRequest(postOp);
        });
    }

    /**
     * Helper method to extract the service payload based on the type of the request and whether it
     * is replicated
     * @param request input request operation
     * @param s service against which the operation is invoked
     * @param clazz service state class
     * @return
     */
    public static <T extends ServiceDocument> T extractBody(Operation request, Service s, Class<T> clazz) {
        T state = null;
        switch (request.getAction()) {
        case PUT:
        case POST:
            // always use the input payload for PUT and POST
            state = request.getBody(clazz);
            break;
        case DELETE:
            // for deletes, a replicated request has the body passed in as part of the request
            if (request.isFromReplication() && request.hasBody()) {
                state = request.getBody(clazz);
            } else {
                state = s.getState(request);
            }
            break;
        default:
            break;
        }
        return state;
    }
}
