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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.xenon.common.QueryFilterUtils;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost.ServiceNotFoundException;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryFilter.QueryFilterException;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;

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

    /**
     * This private {@code Role} class is an encapsulation of the state associated
     * with a single role. The native role service points to a user group and resource
     * group service by reference. As these states are retrieved one after the other,
     * it is convenient to have a wrapper to encapsulate them.
     */
    private static class Role {
        protected RoleState roleState;
        @SuppressWarnings("unused")
        protected UserGroupState userGroupState;
        protected ResourceGroupState resourceGroupState;

        public void setRoleState(RoleState roleState) {
            this.roleState = roleState;
        }

        public void setUserGroupState(UserGroupState userGroupState) {
            this.userGroupState = userGroupState;
        }

        public void setResourceGroupState(ResourceGroupState resourceGroupState) {
            this.resourceGroupState = resourceGroupState;
        }
    }

    public static final String SELF_LINK = ServiceUriPaths.CORE_AUTHZ_VERIFICATION;

    private final Map<String, Collection<Operation>> pendingOperationsBySubject = new HashMap<>();

    /**
     * The service host will invoke this method to allow a service to handle
     * the request in-line or indicate it should be scheduled by service host.
     *
     * @return True if the request has been completed in-line.
     *         False if the request should be scheduled for execution by the service host.
     */
    @Override
    public boolean queueRequest(Operation op) {
        AuthorizationContext ctx = op.getAuthorizationContext();
        if (ctx == null) {
            op.fail(new IllegalArgumentException("no authorization context"));
            return true;
        }

        Claims claims = ctx.getClaims();
        if (claims == null) {
            op.fail(new IllegalArgumentException("no claims"));
            return true;
        }

        String subject = claims.getSubject();
        if (subject == null) {
            op.fail(new IllegalArgumentException("no subject"));
            return true;
        }

        // Allow unconditionally if this is the system user
        if (subject.equals(SystemUserService.SELF_LINK)) {
            op.complete();
            return true;
        }

        // Check whether or not the operation already has a processed context.
        if (ctx.getResourceQueryFilter(op.getAction()) != null) {
            op.complete();
            return true;
        }

        // Needs to be scheduled by the service host, as we'll have to retrieve the
        // user, find out which roles apply, and verify whether the authorization
        // context allows access to the service targeted by the operation.
        return false;
    }

    @Override
    public void handleRequest(Operation op) {
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

        // Add operation to collection of operations for this user.
        // Only if there was no collection for this subject will the routine
        // to gather state and roles for the subject be kicked off.
        synchronized (this.pendingOperationsBySubject) {
            String subject = claims.getSubject();
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

        getSubject(ctx, claims);
    }

    private void getSubject(AuthorizationContext ctx, Claims claims) {
        URI getSubjectUri = UriUtils.buildUri(getHost(), claims.getSubject());
        Operation get = Operation.createGet(getSubjectUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failThrowable(claims.getSubject(), e);
                        return;
                    }

                    ServiceDocument userState = QueryFilterUtils.getServiceState(o, getHost());

                    // If the native user state could not be extracted, we are sure no roles
                    // will apply and we can populate the authorization context.
                    if (userState == null) {
                        populateAuthorizationContext(ctx, claims, null);
                        return;
                    }

                    loadUserGroups(ctx, claims, userState);
                });

        setAuthorizationContext(get, getSystemAuthorizationContext());
        sendRequest(get);
    }

    private void loadUserGroups(AuthorizationContext ctx, Claims claims, ServiceDocument userState) {
        URI getUserGroupsUri = UriUtils.buildUri(getHost(), ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        getUserGroupsUri = UriUtils.buildExpandLinksQueryUri(getUserGroupsUri);
        Operation get = Operation.createGet(getUserGroupsUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failThrowable(claims.getSubject(), e);
                        return;
                    }

                    ServiceDocumentQueryResult result = o
                            .getBody(ServiceDocumentQueryResult.class);
                    Collection<UserGroupState> userGroupStates = new ArrayList<>();
                    for (Object doc : result.documents.values()) {
                        UserGroupState userGroupState = Utils.fromJson(doc,
                                UserGroupState.class);

                        try {
                            QueryFilter f = QueryFilter.create(userGroupState.query);
                            if (QueryFilterUtils.evaluate(f, userState, getHost())) {
                                userGroupStates.add(userGroupState);
                            }
                        } catch (QueryFilterException qfe) {
                            logWarning("Error creating query filter: %s", qfe.toString());
                            failThrowable(claims.getSubject(), qfe);
                            return;
                        }
                    }

                    // If no user groups apply to this user, we are sure no roles
                    // will apply and we can populate the authorization context.
                    if (userGroupStates.isEmpty()) {
                        // TODO(DCP-782): Add negative cache
                        populateAuthorizationContext(ctx, claims, null);
                        return;
                    }

                    loadRoles(ctx, claims, userGroupStates);
                });

        setAuthorizationContext(get, getSystemAuthorizationContext());
        sendRequest(get);
    }

    private void loadRoles(AuthorizationContext ctx, Claims claims,
            Collection<UserGroupState> userGroupStates) {
        // Map user group self-link to user group state so we can lookup in O(1) later
        Map<String, UserGroupState> userGroupStateMap = new HashMap<>();
        for (UserGroupState userGroupState : userGroupStates) {
            userGroupStateMap.put(userGroupState.documentSelfLink, userGroupState);
        }

        // Create query for roles referring any of the specified user groups
        Query kindClause = new Query();
        kindClause.occurance = Occurance.MUST_OCCUR;
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND);
        kindClause.setTermMatchType(MatchType.TERM);
        kindClause.setTermMatchValue(RoleState.KIND);

        Query selfLinkClause = new Query();
        selfLinkClause.occurance = Occurance.MUST_OCCUR;
        if (userGroupStates.size() == 1) {
            selfLinkClause.setTermPropertyName(RoleState.FIELD_NAME_USER_GROUP_LINK);
            selfLinkClause.setTermMatchType(MatchType.TERM);
            selfLinkClause.setTermMatchValue(userGroupStates.iterator().next().documentSelfLink);
        } else {
            for (UserGroupState userGroupState : userGroupStates) {
                Query clause = new Query();
                clause.occurance = Occurance.SHOULD_OCCUR;
                clause.setTermPropertyName(RoleState.FIELD_NAME_USER_GROUP_LINK);
                clause.setTermMatchType(MatchType.TERM);
                clause.setTermMatchValue(userGroupState.documentSelfLink);
                selfLinkClause.addBooleanClause(clause);
            }
        }

        Query query = new Query();
        query.addBooleanClause(kindClause);
        query.addBooleanClause(selfLinkClause);

        QueryTask queryTask = new QueryTask();
        queryTask.querySpec = new QuerySpecification();
        queryTask.querySpec.query = query;
        queryTask.querySpec.options =
                EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
        queryTask.setDirect(true);

        URI postQueryUri = UriUtils.buildUri(getHost(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        Operation post = Operation.createPost(postQueryUri)
                .setBody(queryTask)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failThrowable(claims.getSubject(), e);
                        return;
                    }

                    QueryTask queryTaskResult = o.getBody(QueryTask.class);
                    ServiceDocumentQueryResult result = queryTaskResult.results;
                    if (result.documents == null || result.documents.isEmpty()) {
                        // TODO(DCP-782): Add negative cache
                        failForbidden(claims.getSubject());
                        return;
                    }

                    Collection<Role> roles = new LinkedList<>();
                    for (Object doc : result.documents.values()) {
                        RoleState roleState = Utils.fromJson(doc, RoleState.class);
                        Role role = new Role();
                        role.setRoleState(roleState);
                        role.setUserGroupState(userGroupStateMap.get(roleState.userGroupLink));
                        roles.add(role);
                    }

                    loadResourceGroups(ctx, claims, roles);
                });

        setAuthorizationContext(post, getSystemAuthorizationContext());
        sendRequest(post);
    }

    private void loadResourceGroups(AuthorizationContext ctx, Claims claims, Collection<Role> roles) {
        // Map resource group self-link to role so we can lookup in O(1) later
        Map<String, Collection<Role>> rolesByResourceGroup = new HashMap<>();
        for (Role role : roles) {
            String resourceGroupLink = role.roleState.resourceGroupLink;
            Collection<Role> byResourceGroup = rolesByResourceGroup.get(resourceGroupLink);
            if (byResourceGroup == null) {
                byResourceGroup = new LinkedList<>();
                rolesByResourceGroup.put(resourceGroupLink, byResourceGroup);
            }

            byResourceGroup.add(role);
        }

        JoinedCompletionHandler handler = (ops, failures) -> {
            if (failures != null && !failures.isEmpty()) {
                failThrowable(claims.getSubject(), failures.values().iterator().next());
                return;
            }

            // Add every resource group state to every role that references it
            for (Operation op : ops.values()) {
                ResourceGroupState resourceGroupState = op.getBody(ResourceGroupState.class);
                Collection<Role> rolesForResourceGroup =
                        rolesByResourceGroup.get(resourceGroupState.documentSelfLink);
                for (Role role : rolesForResourceGroup) {
                    role.setResourceGroupState(resourceGroupState);
                }
            }

            populateAuthorizationContext(ctx, claims, roles);
        };

        // Fire off GET for every resource group
        Collection<Operation> gets = new LinkedList<>();
        for (String resourceGroupLink : rolesByResourceGroup.keySet()) {
            Operation get = Operation.createGet(this, resourceGroupLink).setReferer(getUri());
            setAuthorizationContext(get, getSystemAuthorizationContext());
            gets.add(get);
        }

        OperationJoin join = OperationJoin.create(gets);
        join.setCompletion(handler);
        join.sendWith(getHost());
    }

    private void populateAuthorizationContext(AuthorizationContext ctx, Claims claims, Collection<Role> roles) {
        if (roles == null) {
            roles = Collections.emptyList();
        }

        AuthorizationContext.Builder builder = AuthorizationContext.Builder.create();
        builder.setClaims(ctx.getClaims());
        builder.setToken(ctx.getToken());

        if (!roles.isEmpty()) {
            Map<Action, Collection<Role>> roleListByAction = new HashMap<>(Action.values().length);
            for (Role role : roles) {
                for (Action action : role.roleState.verbs) {
                    Collection<Role> roleList = roleListByAction.get(action);
                    if (roleList == null) {
                        roleList = new LinkedList<>();
                        roleListByAction.put(action, roleList);
                    }

                    roleList.add(role);
                }
            }

            Map<Action, QueryFilter> queryFilterByAction = new HashMap<>(Action.values().length);
            Map<Action, Query> queryByAction = new HashMap<>(Action.values().length);
            for (Map.Entry<Action, Collection<Role>> entry : roleListByAction.entrySet()) {
                Query q = new Query();
                q.occurance = Occurance.MUST_OCCUR;
                for (Role role : entry.getValue()) {
                    Query resourceGroupQuery = role.resourceGroupState.query;
                    resourceGroupQuery.occurance = Occurance.SHOULD_OCCUR;
                    q.addBooleanClause(resourceGroupQuery);
                }

                try {
                    queryFilterByAction.put(entry.getKey(), QueryFilter.create(q));
                    queryByAction.put(entry.getKey(), q);
                } catch (QueryFilterException qfe) {
                    logWarning("Error creating query filter: %s", qfe.toString());
                    failThrowable(claims.getSubject(), qfe);
                    return;
                }
            }

            builder.setResourceQueryMap(queryByAction);
            builder.setResourceQueryFilterMap(queryFilterByAction);
        }

        AuthorizationContext newContext = builder.getResult();
        getHost().cacheAuthorizationContext(this, newContext);
        completePendingOperations(claims.getSubject(), newContext);
    }

    private Collection<Operation> getPendingOperations(String subject) {
        Collection<Operation> operations;

        synchronized (this.pendingOperationsBySubject) {
            operations = this.pendingOperationsBySubject.get(subject);
            this.pendingOperationsBySubject.remove(subject);
        }

        if (operations == null) {
            return Collections.emptyList();
        }

        return operations;
    }

    private void completePendingOperations(String subject, AuthorizationContext ctx) {
        for (Operation op : getPendingOperations(subject)) {
            setAuthorizationContext(op, ctx);
            op.complete();
        }
    }

    private void failThrowable(String subject, Throwable e) {
        if (e instanceof ServiceNotFoundException) {
            failNotFound(subject);
            return;
        }
        for (Operation op : getPendingOperations(subject)) {
            op.fail(e);
        }
    }

    private void failForbidden(String subject) {
        for (Operation op : getPendingOperations(subject)) {
            op.fail(Operation.STATUS_CODE_FORBIDDEN);
        }
    }

    private void failNotFound(String subject) {
        for (Operation op : getPendingOperations(subject)) {
            op.fail(Operation.STATUS_CODE_NOT_FOUND);
        }
    }
}
