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

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.vmware.xenon.common.AuthUtils;
import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.xenon.common.QueryFilterUtils;
import com.vmware.xenon.common.ReflectionUtils;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
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
import com.vmware.xenon.services.common.RoleService.Policy;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;
import com.vmware.xenon.services.common.UserService.UserState;

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
    private final Set<String> cacheClearRequests = Collections.synchronizedSet(new HashSet<>());
    // map of service factories to document description. This is local to the class at this time as
    // we do not track anonymous factory classes in the ServiceHost description cache
    // This can be moved there as part of a larger refactoring
    private final ConcurrentHashMap<String, ServiceDocumentDescription> factoryDescriptionMap =
                                        new ConcurrentHashMap<>();

    /**
     * The service host will invoke this method to allow a service to handle
     * the request in-line or indicate it should be scheduled by service host.
     *
     * @return True if the request has been completed in-line.
     *         False if the request should be scheduled for execution by the service host.
     */
    @Override
    public boolean queueRequest(Operation op) {
        if (op.getAction() == Action.DELETE && op.getUri().getPath().equals(getSelfLink())) {
            return false;
        }
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

        // handle a cache clear request
        if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CLEAR_AUTH_CACHE)) {
            return handleCacheClearRequest(op, subject);
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

    private boolean handleCacheClearRequest(Operation op, String subject) {
        AuthorizationCacheClearRequest requestBody = op.getBody(AuthorizationCacheClearRequest.class);
        if (!AuthorizationCacheClearRequest.KIND.equals(requestBody.kind)) {
            op.fail(new IllegalArgumentException("invalid request body type"));
            return true;
        }
        if (requestBody.subjectLink == null) {
            op.fail(new IllegalArgumentException("no subjectLink"));
            return true;
        }
        if (!subject.equals(SystemUserService.SELF_LINK)) {
            op.fail(Operation.STATUS_CODE_FORBIDDEN);
            return true;
        }
        synchronized (this.pendingOperationsBySubject) {
            if (this.pendingOperationsBySubject.containsKey(requestBody.subjectLink)) {
                this.cacheClearRequests.add(requestBody.subjectLink);
            }
        }
        getHost().clearAuthorizationContext(this, requestBody.subjectLink);
        op.complete();
        return true;
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
        URI getSubjectUri = AuthUtils.buildUserUriFromClaims(getHost(), claims);
        Operation get = Operation.createGet(getSubjectUri)
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failThrowable(claims.getSubject(), e);
                        return;
                    }
                    ServiceDocument userState = extractServiceState(o);
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

    private ServiceDocument extractServiceState(Operation getOp) {
        ServiceDocument userState = QueryFilterUtils.getServiceState(getOp, getHost());
        if (userState == null) {
            Object rawBody = getOp.getBodyRaw();
            Class<?> serviceTypeClass = null;
            if (rawBody instanceof String) {
                String kind = Utils.getJsonMapValue(rawBody, ServiceDocument.FIELD_NAME_KIND,
                        String.class);
                serviceTypeClass = Utils.getTypeFromKind(kind);
            } else {
                serviceTypeClass = rawBody.getClass();
            }
            if (serviceTypeClass != null) {
                userState = (ServiceDocument)Utils.fromJson(rawBody, serviceTypeClass);
            }
        }
        return userState;
    }

    private boolean loadUserGroupsFromUserState(AuthorizationContext ctx, Claims claims, ServiceDocument userServiceDocument) {
        Field groupLinksField = ReflectionUtils.getFieldIfExists(userServiceDocument.getClass(),
                UserState.FIELD_NAME_USER_GROUP_LINKS);

        if (groupLinksField == null) {
            return false;
        }

        Object fieldValue;
        try {
            fieldValue = groupLinksField.get(userServiceDocument);
        } catch (IllegalArgumentException | IllegalAccessException e1) {
            return false;
        }
        if (!(fieldValue instanceof Collection<?>)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        Collection<String> userGroupLinks = (Collection<String>) fieldValue;
        if (userGroupLinks.isEmpty()) {
            return false;
        }
        JoinedCompletionHandler handler = (ops, failures) -> {
            Collection<Operation> userGroupOps = null;
            if (failures != null && !failures.isEmpty()) {
                userGroupOps = new HashSet<>(ops.values());
                for (Operation groupOp : ops.values()) {
                    if (groupOp.getStatusCode() == Operation.STATUS_CODE_OK) {
                        continue;
                    } else if (groupOp.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                        userGroupOps.remove(groupOp);
                        continue;
                    }
                    failThrowable(claims.getSubject(), failures.values().iterator().next());
                    return;
                }
            } else {
                userGroupOps = ops.values();
            }

            // If no user groups apply to this user, we are sure no roles
            // will apply and we can populate the authorization context with a null context.
            if (userGroupOps.isEmpty()) {
                populateAuthorizationContext(ctx, claims, null);
                return;
            }
            try {
                Collection<UserGroupState> userGroupStates = new HashSet<>();
                for (Operation op : userGroupOps) {
                    UserGroupState userGroupState = op.getBody(UserGroupState.class);
                    userGroupStates.add(userGroupState);
                }
                loadRoles(ctx, claims, userGroupStates);
            } catch (Throwable e) {
                failThrowable(claims.getSubject(), e);
                return;
            }
        };
        Collection<Operation> gets = new HashSet<>();
        for (String userGroupLink : userGroupLinks) {
            Operation get = Operation.createGet(AuthUtils.buildAuthProviderHostUri(getHost(), userGroupLink)).setReferer(getUri());
            setAuthorizationContext(get, getSystemAuthorizationContext());
            gets.add(get);
        }
        OperationJoin join = OperationJoin.create(gets);
        join.setCompletion(handler);
        join.sendWith(getHost());
        return true;
    }

    private void loadUserGroups(AuthorizationContext ctx, Claims claims, ServiceDocument userServiceDocument) {
        // if the user service derived from UserService and it has the userGroupLinks
        // field populated, use that to compute the list of groups that apply to the user
        if (loadUserGroupsFromUserState(ctx, claims, userServiceDocument)) {
            return;
        }

        URI getUserGroupsUri = AuthUtils.buildAuthProviderHostUri(getHost(), ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        getUserGroupsUri = UriUtils.buildExpandLinksQueryUri(getUserGroupsUri);
        Operation get = Operation.createGet(getUserGroupsUri)
                .setConnectionSharing(true)
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
                            ServiceDocumentDescription sdd = getServiceDesc(userServiceDocument);
                            if (QueryFilterUtils.evaluate(f, userServiceDocument, sdd)) {
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

    private ServiceDocumentDescription getServiceDesc(ServiceDocument userServiceDocument) {
        String parentLink = UriUtils.getParentPath(userServiceDocument.documentSelfLink);
        if (parentLink == null) {
            return null;
        }
        ServiceDocumentDescription sdd =
                this.factoryDescriptionMap.computeIfAbsent(parentLink, (val) -> {
                    return ServiceDocumentDescription.Builder.create()
                            .buildDescription(userServiceDocument.getClass());
                });
        return sdd;
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

        URI postQueryUri = AuthUtils.buildAuthProviderHostUri(getHost(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        Operation post = Operation.createPost(postQueryUri)
                .setBody(queryTask)
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failThrowable(claims.getSubject(), e);
                        return;
                    }

                    QueryTask queryTaskResult = o.getBody(QueryTask.class);
                    ServiceDocumentQueryResult result = queryTaskResult.results;
                    if (result.documents == null || result.documents.isEmpty()) {
                        populateAuthorizationContext(ctx, claims, null);
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
            Collection<Operation> resourceGroupOps = null;
            if (failures != null && !failures.isEmpty()) {
                resourceGroupOps = new HashSet<>(ops.values());
                for (Operation getOp : ops.values()) {
                    if (getOp.getStatusCode() == Operation.STATUS_CODE_OK) {
                        continue;
                    } else if (getOp.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                        // ignore ops resulting in a 404 response code
                        resourceGroupOps.remove(getOp);
                        continue;
                    }
                    failThrowable(claims.getSubject(), failures.values().iterator().next());
                    return;
                }
            } else {
                resourceGroupOps = ops.values();
            }

            try {
                // Add every resource group state to every role that references it
                for (Operation op : resourceGroupOps) {
                    ResourceGroupState resourceGroupState = op.getBody(ResourceGroupState.class);
                    Collection<Role> rolesForResourceGroup = rolesByResourceGroup
                            .get(resourceGroupState.documentSelfLink);
                    if (rolesForResourceGroup != null) {
                        for (Role role : rolesForResourceGroup) {
                            role.setResourceGroupState(resourceGroupState);
                        }
                    }
                }
                populateAuthorizationContext(ctx, claims, roles);
            } catch (Throwable e) {
                failThrowable(claims.getSubject(), e);
                return;
            }
        };

        // Fire off GET for every resource group
        Collection<Operation> gets = new LinkedList<>();
        for (String resourceGroupLink : rolesByResourceGroup.keySet()) {
            Operation get = Operation.createGet(AuthUtils.buildAuthProviderHostUri(getHost(),
                                resourceGroupLink)).setReferer(getUri());
            setAuthorizationContext(get, getSystemAuthorizationContext());
            gets.add(get);
        }

        OperationJoin join = OperationJoin.create(gets);
        join.setCompletion(handler);
        join.sendWith(getHost());
    }

    private void populateAuthorizationContext(AuthorizationContext ctx, Claims claims,
            Collection<Role> roles) {
        if (roles == null) {
            roles = Collections.emptyList();
        }
        try {

            AuthorizationContext.Builder builder = AuthorizationContext.Builder.create();
            builder.setClaims(ctx.getClaims());
            builder.setToken(ctx.getToken());

            if (!roles.isEmpty()) {
                Map<Action, Collection<Role>> roleListByAction = new HashMap<>(
                        Action.values().length);
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

                Map<Action, QueryFilter> queryFilterByAction = new HashMap<>(
                        Action.values().length);
                Map<Action, Query> queryByAction = new HashMap<>(Action.values().length);
                for (Map.Entry<Action, Collection<Role>> entry : roleListByAction.entrySet()) {
                    Query q = new Query();
                    q.occurance = Occurance.MUST_OCCUR;
                    for (Role role : entry.getValue()) {
                        if (role.resourceGroupState == null) {
                            continue;
                        }
                        Query resourceGroupQuery = role.resourceGroupState.query;
                        if (role.roleState.policy == Policy.ALLOW) {
                            resourceGroupQuery.occurance = Occurance.SHOULD_OCCUR;
                        } else {
                            resourceGroupQuery.occurance = Occurance.MUST_NOT_OCCUR;
                        }
                        q.addBooleanClause(resourceGroupQuery);
                    }

                    if (q.booleanClauses != null) {
                        try {
                            queryFilterByAction.put(entry.getKey(), QueryFilter.create(q));
                            queryByAction.put(entry.getKey(), q);
                        } catch (QueryFilterException qfe) {
                            logWarning("Error creating query filter: %s", qfe.toString());
                            failThrowable(claims.getSubject(), qfe);
                            return;
                        }
                    }
                }

                builder.setResourceQueryMap(queryByAction);
                builder.setResourceQueryFilterMap(queryFilterByAction);
            }

            AuthorizationContext newContext = builder.getResult();
            boolean retry = false;
            if (this.cacheClearRequests.remove(claims.getSubject())) {
                retry = true;
            }
            if (retry) {
                getSubject(ctx, claims);
            } else {
                getHost().cacheAuthorizationContext(this, newContext);
                completePendingOperations(claims.getSubject(), newContext);
            }
        } catch (Throwable e) {
            failThrowable(claims.getSubject(), e);
        }
    }

    private Collection<Operation> getPendingOperations(String subject) {
        Collection<Operation> operations;
        synchronized (this.pendingOperationsBySubject) {
            operations = this.pendingOperationsBySubject.remove(subject);
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

    private void failNotFound(String subject) {
        for (Operation op : getPendingOperations(subject)) {
            op.fail(Operation.STATUS_CODE_NOT_FOUND);
        }
    }
}
