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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;



import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;

public class AuthorizationCacheUtils {

    /**
     * Helper method that nests a completion to clear the service host authz cache for the specified user service.
     * {@code userPath} is retrieved from provided operation. {@code op.getUri().getPath();}
     * The nested completion will run after the operation passed in has been marked complete outside of this
     * method.
     * @param s service context to invoke the operation
     * @param op Operation to mark completion/failure
     */
    public static void clearAuthzCacheForUser(Service s,  Operation op) {
        String userPath = op.getUri().getPath();
        op.nestCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            if (s.getHost().getAuthorizationServiceUri() == null) {
                op.complete();
                return;
            }
            AuthorizationCacheClearRequest request = AuthorizationCacheClearRequest.create(userPath);
            Operation postClearCacheRequest = Operation.createPost(s.getHost(), ServiceUriPaths.CORE_AUTHZ_VERIFICATION)
                    .setBody(request)
                    .setCompletion((clearOp, clearEx) -> {
                        if (clearEx != null) {
                            s.getHost().log(Level.SEVERE, Utils.toString(clearEx));
                            op.fail(clearEx);
                            return;
                        }
                        op.complete();
                    });
            postClearCacheRequest.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_CLEAR_AUTH_CACHE);
            s.setAuthorizationContext(postClearCacheRequest, s.getSystemAuthorizationContext());
            s.sendRequest(postClearCacheRequest);
        });
    }

    /**
     * Helper method that nests a completion to clear the service host authz cache for all
     * services that a UserGroup service query resolves to.
     * The nested completion will run after the operation passed in has been marked complete outside of this
     * method.
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
                        AtomicInteger completionCount = new AtomicInteger(0);
                        CompletionHandler handler = (clearOp, clearEx) -> {
                            if (clearEx != null) {
                                s.getHost().log(Level.SEVERE, Utils.toString(clearEx));
                                op.fail(clearEx);
                                return;
                            }
                            if (completionCount.incrementAndGet() == result.documentLinks.size()) {
                                op.complete();
                            }
                        };
                        for (String userLink : result.documentLinks) {
                            Operation clearUserOp = new Operation();
                            clearUserOp.setUri(UriUtils.buildUri(s.getHost(), userLink));
                            clearUserOp.setCompletion(handler);
                            clearAuthzCacheForUser(s, clearUserOp);
                            clearUserOp.complete();
                        }
                    }
                );
            s.setAuthorizationContext(postOp, s.getSystemAuthorizationContext());
            s.sendRequest(postOp);
        });
    }

    /**
     * Helper method that nests a completion to clear the service host authz cache for all
     * services that a Role service resolves to. A Role has a reference
     * to a UserGroup instance which points to users.
     * The nested completion will run after the operation passed in has been marked complete outside of this
     * method.
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
     * {@code resourceGroupPath} is retrieved from provided operation. {@code op.getUri().getPath();}
     * The nested completion will run after the operation passed in has been marked complete outside of this
     * method
     * @param s service context to invoke the operation
     * @param op Operation to mark completion/failure
     */
    public static void clearAuthzCacheForResourceGroup(Service s, Operation op) {

        String resourceGroupPath = op.getUri().getPath();

        op.nestCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            QueryTask queryTask = new QueryTask();
            queryTask.querySpec = new QuerySpecification();
            Query resourceGroupQuery = Builder.create()
                    .addFieldClause(RoleState.FIELD_NAME_RESOURCE_GROUP_LINK, resourceGroupPath)
                    .addKindFieldClause(RoleState.class)
                    .build();
            queryTask.querySpec.options =
                    EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
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
     * For PATCH request, this method does NOT provide any implementation. The caller should decide
     * how to handle PATCH request.
     *
     * @param request input request operation
     * @param s service against which the operation is invoked
     * @param clazz service state class
     */
    public static <T extends ServiceDocument> T extractBody(Operation request, Service s, Class<T> clazz) {
        // If this was a synchronization request to the document
        // owner node, the request will not contain a body.
        if (!request.hasBody() &&
                request.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_OWNER)) {
            return null;
        }

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

    public static boolean isAuthzCacheClearApplicableOperation(Operation op) {

        // do not clear cache for GET request
        if (op.getAction() == Action.GET) {
            return false;
        }

        // For replication requests, create(POST) comes through factory service and it is not two
        // phased.
        // For PUT/PATCH, when requests are pending, it does NOT issue explicit commit.
        // the next update(version N+1) is an implicit commit for version N.
        // Therefore, we eagerly clear auth cache for PUT/PATCH.
        // On the other hand, DELETE is always two phased. Therefore, only clear the auth cache at commit phase.
        if (op.isFromReplication()) {
            if (op.getAction() == Action.POST) {
                if (!op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED)) {
                    // do not clear at restart.
                    return false;
                }
            } else if (op.getAction() == Action.DELETE) {
                if (!op.isCommit()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Method to remove a clause from a query. This method bases
     * an equality check of the clause on the term and occurance
     * fields in the query. Any boolean clauses in the query are
     * not considered
     * @param inputQuery The input query to remove the clause
     * @param inputClause The clause to remove
     */
    public static Query removeBooleanClause(Query inputQuery, Query inputClause) {
        if (inputQuery.booleanClauses == null || inputClause == null) {
            return inputQuery;
        }
        for (Query clause : inputQuery.booleanClauses) {
            if (Objects.equals(clause.term, inputClause.term)
                    && Objects.equals(clause.occurance, inputClause.occurance)) {
                inputQuery.booleanClauses.remove(clause);
                break;
            }
        }
        return inputQuery;
    }

}
